"""
Renewal / at-risk classification (module: ``renewal_classifier``, V6 framework).
Commerce Cloud ONLY — FY2027 + FY2028
Triggered automatically on Google Sheets export (``export_to_gsheet``),
e.g. from ``/gm-review-sheet`` or canvas flows that export.
"""

import logging
import os
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from domain.salesforce.org62_client import Org62Client, _escape

logger = logging.getLogger(__name__)

# Narrative-derived signals (Issues / threat text ≈ CRM risk notes in `_fetch_crm_data`).
_SERVICE_RECOVERY_LITERALS = (
    "adoption",
    "utilization",
    "success plan",
    "platform",
    "instability",
    "perceived value",
    "product-market",
    "product market",
    "oversold",
    "pricing",
    "cost justification",
)
_MISC_NON_RECOVERABLE_PHRASES = (
    "business direction change",
    "no longer relevant",
    "shutting down",
    "bankruptcy",
    "divest",
    "product no longer needed",
)
_COMP_FINAL_PHRASES = (
    "signed with",
    "signed contract",
    "competitive",
    "switched to",
    "replaced with",
    "direct competitor",
    "in-house solution",
)


_ALREADY_MIGRATING_LITERALS = (
    "in-house",
    "migrating",
    "migration in progress",
    "go-live",
)
_RFP_LITERALS = (
    "rfp",
    "request for proposal",
    "issued rfp",
    "running rfp",
)
_EXEC_ESCALATION_LITERALS = (
    "ebc",
    "exec escalation",
    "executive escalation",
    "escalated to",
)


def _truthy_red_ac_flag(raw: Any) -> bool:
    if raw is True:
        return True
    if raw in (1, "1"):
        return True
    s = str(raw or "").strip().lower()
    if not s:
        return False
    if s in ("yes", "true"):
        return True
    return s.startswith("yes")


def _burn_rate_float(row: Any) -> float | None:
    """Numeric commerce burn rate from classifier row (sheet export or numeric field)."""
    if row is None or not isinstance(row, dict):
        return None
    v = row.get("burn_rate")
    if v is None or v == "":
        return None
    if isinstance(v, str) and v.strip().upper() == "N/A":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _renewal_days_from_today(renewal_close_dt: str) -> int | None:
    """
    Days from today to renewal month (or full date). Returns None if unparseable.
    Prefer YYYY-MM; also accepts YYYY-MM-DD prefixes.
    """
    s = (renewal_close_dt or "").strip()
    if not s:
        return None
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            end = datetime.strptime(s[:10], "%Y-%m-%d")
        else:
            key = s[:7]
            if len(key) < 7 or key[4] != "-":
                return None
            end = datetime.strptime(key, "%Y-%m")
        return (end - datetime.now()).days
    except ValueError:
        return None


CLASSIFICATION_VALUES = [
    "Actionable",
    "Actionable — AOVPP",
    "Actionable — Renewals + Product",
    "Actionable — URGENT",
    "Non-Actionable — Signed with Competitor",
    "Non-Actionable — Already Migrating",
    "Non-Actionable — KMOD",
    "Non-Actionable — Macro / M&A",
    "Non-Actionable — Miscellaneous",
    "Already Attrited",
    "Pending Review",
]


@dataclass
class ClassificationResult:
    opp_id: str
    account_nm: str
    csg_territory: str
    attrition: float
    swing_amount: float
    renewal_close_dt: str
    recommendation: str  # exact V6 dropdown string
    rule_applied: str  # e.g. "Rule 0", "Pre-S1"
    signal_source: str  # Sheet | Slack | Org62 | Both
    reasoning: str  # 1-2 sentence explanation
    classified_by: str = "bot"
    classified_at: str = ""

    def __post_init__(self):
        if not self.classified_at:
            self.classified_at = datetime.now(timezone.utc).isoformat()


class ClassifyRenewalWorkflow:
    """V6 classifier; requires Slack ``WebClient`` (or Bolt ``client`` shim) + Org62 client."""

    def __init__(
        self,
        slack_client: Any,
        org62_client: Org62Client,
        db_path: str = "bot_history.db",
    ):
        self.slack = slack_client
        self.org62 = org62_client
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS account_classification (
                    opp_id           TEXT PRIMARY KEY,
                    account_nm       TEXT,
                    csg_territory    TEXT,
                    attrition        REAL,
                    swing_amount     REAL,
                    renewal_close_dt TEXT,
                    recommendation   TEXT,
                    rule_applied     TEXT,
                    signal_source    TEXT,
                    reasoning        TEXT,
                    classified_by    TEXT,
                    classified_at    TEXT,
                    last_updated     TEXT
                )
                """
            )
            conn.commit()

    # ─────────────────────────────────────────────────────────────────────────
    # MAIN ENTRY POINT — call once per exported row after ``append_rows()``
    # ─────────────────────────────────────────────────────────────────────────
    def classify(
        self,
        row: dict,
        record_channel_id: Optional[str] = None,
    ) -> ClassificationResult:
        self._active_classify_row = row
        opp_id = row.get("opp_id", "")
        account_nm = row.get("account_nm", "")
        csg_territory = row.get("csg_territory", "")
        attrition = float(row.get("forecasted_attrition") or 0)
        swing_amount = float(row.get("swing_amount") or 0)
        renewal_dt = row.get("renewal_month", "") or ""

        # Rule 0: Swing Amount positive → immediately Actionable
        if swing_amount > 0:
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Actionable",
                rule_applied="Rule 0 — Swing Amount Positive",
                signal_source="Sheet",
                reasoning=(
                    f"Swing Amount is ${swing_amount:,.0f} (positive). "
                    "Account has upside — immediately Actionable."
                ),
            )

        slack_text = ""
        if record_channel_id:
            slack_text = self._fetch_slack_signals(
                record_channel_id, account_nm, opp_id
            )
        slack_lower = slack_text.lower()

        competitor_name = self._extract_competitor(slack_lower)
        if competitor_name or any(
            s in slack_lower for s in ["#non-actionable", "migrated to", "live on"]
        ):
            label = competitor_name or "competitor/migration signal"
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Non-Actionable — Signed with Competitor",
                rule_applied="Pre-S1 — Slack Non-Actionable Signal",
                signal_source="Slack",
                reasoning=(
                    f"Slack signals confirm non-actionable: '{label}' "
                    "detected in last 30 days."
                ),
            )

        if any(s in slack_lower for s in ["nco closed", "ramp down approved"]):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Already Attrited",
                rule_applied="Pre-Attrited — Slack Signal",
                signal_source="Slack",
                reasoning=(
                    "Slack confirms account already attrited "
                    "('NCO closed' or 'ramp down approved')."
                ),
            )

        slack_actionable_lean = any(
            s in slack_lower for s in ["#actionable", "we can save this"]
        )

        crm = self._fetch_crm_data(opp_id, row)

        if crm.get("is_past_close_date"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Already Attrited",
                rule_applied="Rule 1 — Already Attrited (Past Close Date)",
                signal_source="Org62",
                reasoning=(
                    f"Close Date {crm.get('close_date')} is in the past. "
                    "Account already attrited."
                ),
            )

        if crm.get("competitor_confirmed_final"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Non-Actionable — Signed with Competitor",
                rule_applied="Rule 2 — Competitor Confirmed (CRM)",
                signal_source="Org62",
                reasoning=(
                    f"CRM confirms final competitor decision: "
                    f"{crm.get('competitor_name', 'competitor')}."
                ),
            )

        if crm.get("is_macro_ma"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Non-Actionable — Macro / M&A",
                rule_applied="Rule 3 — Macro / M&A",
                signal_source="Org62",
                reasoning=(
                    f"CRM indicates macro/M&A risk: "
                    f"{crm.get('risk_detail', 'M&A or economic event detected')}."
                ),
            )

        if crm.get("is_already_migrating"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Non-Actionable — Already Migrating",
                rule_applied="Rule 2b — Already Migrating",
                signal_source="Org62",
                reasoning=(
                    "CRM narrative indicates in-house move, migration in progress, or go-live "
                    "off Salesforce — renewal not actionable on standard save path."
                ),
            )

        if crm.get("is_actionable_service_risk"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Actionable — Renewals + Product",
                rule_applied="Rule 4a — Recoverable Service Risk",
                signal_source="Org62",
                reasoning=(
                    "CRM narrative shows recoverable adoption, utilization, pricing, "
                    "or similar service/product-context risk — routed to Renewals + Product."
                ),
            )

        if crm.get("is_rfp"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Actionable — Renewals + Product",
                rule_applied="Rule 4b — RFP Issued",
                signal_source="Org62",
                reasoning=(
                    "RFP / competitive sourcing language detected — Renewals + Product "
                    "engagement appropriate."
                ),
            )

        if crm.get("is_red_account") and not crm.get(
            "competitor_confirmed_final"
        ):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Actionable — Renewals + Product",
                rule_applied="Rule 4c — Red Account Flag",
                signal_source="Sheet",
                reasoning=(
                    "Red AC Flag is set and competitor outcome is not confirmed in CRM narrative."
                ),
            )

        br_crm = crm.get("burn_rate")
        if (
            br_crm is not None
            and 0.5 <= br_crm < 0.9
            and not crm.get("is_exec_escalation")
        ):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Actionable — Renewals + Product",
                rule_applied="Rule 4f — Low Burn Rate",
                signal_source="Sheet",
                reasoning=(
                    f"Commerce burn rate {br_crm:.2f} is between 0.5 and 0.9 — adoption "
                    "trajectory warrants Renewals + Product engagement."
                ),
            )

        if crm.get("is_exec_escalation"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Actionable — Renewals + Product",
                rule_applied="Rule 4e — Exec Escalation / EBC",
                signal_source="Org62",
                reasoning=(
                    "Executive escalation, EBC, or similar leadership touchpoint noted in CRM."
                ),
            )

        if crm.get("is_miscellaneous_non_renewal"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Non-Actionable — Miscellaneous",
                rule_applied="Rule 4 — Miscellaneous Non-Renewal",
                signal_source="Org62",
                reasoning=(
                    "CRM cites a structural / non-recoverable non-renewal reason "
                    "(e.g. direction change, no longer relevant, shutdown, divestment, "
                    "product no longer needed), not recoverable service risk."
                ),
            )

        if crm.get("is_kmod_only"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Non-Actionable — KMOD",
                rule_applied="Rule 5 — KMOD Only Path",
                signal_source="Org62",
                reasoning=(
                    "CRM confirms only viable path is right-sizing (KMOD). "
                    "Not actionable for full save."
                ),
            )

        if crm.get("is_aovpp"):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Actionable — AOVPP",
                rule_applied="Rule 6 — AOVPP Enrolled",
                signal_source="Org62",
                reasoning=(
                    "Account is actively enrolled in the AOV Protection Program."
                ),
            )

        if crm.get("competitor_under_evaluation") or crm.get(
            "executive_skeptical"
        ):
            return self._result(
                opp_id,
                account_nm,
                csg_territory,
                attrition,
                swing_amount,
                renewal_dt,
                recommendation="Actionable — Renewals + Product",
                rule_applied=(
                    "Rule 7 — Competitor Under Evaluation / Exec Skeptical"
                ),
                signal_source="Org62",
                reasoning=(
                    "Competitor under evaluation or executive skepticism detected. "
                    "Renewals + Product team engagement needed."
                ),
            )

        source = "Slack + Org62" if slack_actionable_lean else "Org62"
        return self._result(
            opp_id,
            account_nm,
            csg_territory,
            attrition,
            swing_amount,
            renewal_dt,
            recommendation="Actionable",
            rule_applied="Rule 8 — Default Actionable",
            signal_source=source,
            reasoning=(
                "Future close date, no confirmed competitor, active next steps. "
                "Default Actionable."
            ),
        )

    def _augment_reasoning_for_burn(
        self, reasoning: str, recommendation: str, row: dict
    ) -> str:
        """Rule 4f modifiers: critical burn strengthens Non-Actionable; healthy burn boosts Actionable."""
        br = _burn_rate_float(row)
        if br is None:
            return reasoning
        rec = (recommendation or "").strip()
        base = (reasoning or "").rstrip()
        if br < 0.5 and rec.startswith("Non-Actionable"):
            return (
                f"{base} Commerce burn rate {br:.2f} is critically low (<0.5), "
                "reinforcing a constrained recovery outlook."
            )
        if br >= 0.9 and rec.startswith("Actionable"):
            return (
                f"{base} Strong commerce burn rate ({br:.2f} ≥ 0.9) supports the save motion."
            )
        return reasoning or ""

    def _maybe_urgent_actionable(
        self,
        renewal_close_dt: str,
        recommendation: str,
        rule_applied: str,
        reasoning: str,
    ) -> tuple[str, str, str]:
        """Rule 7b — narrow Actionable recommendations to URGENT when renewal ≤90 days."""
        if not (recommendation or "").startswith("Actionable"):
            return recommendation, rule_applied, reasoning
        if recommendation == "Actionable — URGENT":
            return recommendation, rule_applied, reasoning
        days = _renewal_days_from_today(renewal_close_dt)
        if days is None or days > 90:
            return recommendation, rule_applied, reasoning
        return (
            "Actionable — URGENT",
            "Rule 7b — Actionable URGENT (≤90 days)",
            f"{reasoning} Renewal within 90-day window — flagged URGENT.",
        )

    def _result(
        self,
        opp_id,
        account_nm,
        csg_territory,
        attrition,
        swing_amount,
        renewal_close_dt,
        **kwargs,
    ) -> ClassificationResult:
        rec, rule, rsn = self._maybe_urgent_actionable(
            renewal_close_dt,
            kwargs.get("recommendation", ""),
            kwargs.get("rule_applied", ""),
            kwargs.get("reasoning", ""),
        )
        kwargs["recommendation"] = rec
        kwargs["rule_applied"] = rule
        row_ctx = getattr(self, "_active_classify_row", None) or {}
        kwargs["reasoning"] = self._augment_reasoning_for_burn(rsn, rec, row_ctx)
        result = ClassificationResult(
            opp_id=opp_id,
            account_nm=account_nm,
            csg_territory=csg_territory,
            attrition=attrition,
            swing_amount=swing_amount,
            renewal_close_dt=renewal_close_dt,
            **kwargs,
        )
        self._persist(result)
        return result

    def _persist(self, r: ClassificationResult):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO account_classification
                (opp_id, account_nm, csg_territory, attrition, swing_amount,
                 renewal_close_dt, recommendation, rule_applied, signal_source,
                 reasoning, classified_by, classified_at, last_updated)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    r.opp_id,
                    r.account_nm,
                    r.csg_territory,
                    r.attrition,
                    r.swing_amount,
                    r.renewal_close_dt,
                    r.recommendation,
                    r.rule_applied,
                    r.signal_source,
                    r.reasoning,
                    r.classified_by,
                    r.classified_at,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()

    def _fetch_slack_signals(
        self, channel_id: str, account_nm: str, opp_id: str
    ) -> str:
        slack_api = getattr(self.slack, "client", None) or self.slack
        try:
            result = slack_api.conversations_history(
                channel=channel_id,
                oldest=str(time.time() - 30 * 86400),
                limit=200,
            )
            return " ".join(m.get("text", "") for m in result.get("messages", []))
        except Exception as e:
            logger.warning(
                "Slack signal fetch failed for account=%s opp=%s channel=%s: %s",
                account_nm,
                opp_id,
                channel_id,
                e,
            )
            return ""

    def _fetch_crm_data(
        self, opp_id: str, row: Optional[dict] = None
    ) -> dict[str, Any]:
        row = row or {}
        raw_red = row.get("red_ac_flag", row.get("red_flag"))
        br_sheet = _burn_rate_float(row)

        def _sheet_only_base() -> dict[str, Any]:
            return {
                "close_date": "",
                "is_past_close_date": False,
                "competitor_confirmed_final": False,
                "competitor_name": "",
                "competitor_under_evaluation": False,
                "is_macro_ma": False,
                "is_actionable_service_risk": False,
                "is_miscellaneous_non_renewal": False,
                "is_kmod_only": False,
                "is_aovpp": False,
                "executive_skeptical": False,
                "risk_detail": "",
                "is_already_migrating": False,
                "is_rfp": False,
                "is_red_account": _truthy_red_ac_flag(raw_red),
                "burn_rate": br_sheet,
                "is_exec_escalation": False,
            }

        if not (opp_id or "").strip():
            return _sheet_only_base()

        oid = _escape(opp_id.strip())
        raw_fields = os.getenv("CLASSIFY_OPPORTUNITY_SOQL_FIELDS", "").strip()
        default_fields = (
            "Id, CloseDate, StageName, ForecastCategoryName, NextStep, "
            "License_At_Risk_Reason__c, ACV_Reason_Detail__c, Description, "
            "Specialist_Sales_Notes__c, PAM_Comment__c"
        )
        fields_sql = raw_fields if raw_fields else default_fields
        try:
            rows = self.org62.query(
                f"SELECT {fields_sql} FROM Opportunity WHERE Id = '{oid}' LIMIT 1"
            )
            if not rows:
                return _sheet_only_base()

            o = rows[0]

            def _part(v: Any) -> str:
                return str(v or "").strip()

            lr = _part(o.get("License_At_Risk_Reason__c"))
            acv_d = _part(o.get("ACV_Reason_Detail__c"))
            desc = _part(o.get("Description"))
            sales_notes = _part(o.get("Specialist_Sales_Notes__c"))
            pam = _part(o.get("PAM_Comment__c"))
            next_step = _part(o.get("NextStep"))
            stage = _part(o.get("StageName")).lower()
            fc = _part(o.get("ForecastCategoryName")).lower()

            narrative = " ".join(
                [
                    lr.lower(),
                    acv_d.lower(),
                    desc.lower(),
                    sales_notes.lower(),
                    pam.lower(),
                    next_step.lower(),
                    stage,
                    fc,
                ]
            )

            close_date = o.get("CloseDate", "")

            today = datetime.now().strftime("%Y-%m-%d")
            is_past = bool(close_date and close_date < today)
            competitor_final = (
                "closed lost" in stage
                or "loss" in fc
                or fc == "omit"
                or any(p in narrative for p in _COMP_FINAL_PHRASES)
            )
            comp_eval = (
                "evaluat" in narrative
                or "considering" in narrative
                or "at risk" in narrative
                or "competitor" in narrative
            )
            is_ma = any(
                kw in narrative
                for kw in ("m&a", "acquisition", "acquired", "bankruptcy", "divest")
            )
            is_service_risk = any(
                literal in narrative for literal in _SERVICE_RECOVERY_LITERALS
            ) or bool(re.search(r"\broi\b", narrative))
            is_misc = (
                any(p in narrative for p in _MISC_NON_RECOVERABLE_PHRASES)
                and not competitor_final
                and not is_ma
                and not is_service_risk
            )
            is_kmod = (
                "kmod" in narrative
                or "right-size" in narrative
                or "right size" in narrative
                or "reduce" in narrative
            )
            is_aovpp = "aovpp" in narrative or "aov protection" in narrative
            exec_skeptical = (
                "skeptic" in narrative
                or bool(re.search(r"\bcdo\b", narrative))
                or bool(re.search(r"\bcto\b", narrative))
            )
            risk_detail = " ".join(
                filter(None, [lr.lower(), acv_d.lower()])
            ).strip()
            competitor_name = _part(self._extract_competitor(narrative))

            is_already_migrating = any(
                x in narrative for x in _ALREADY_MIGRATING_LITERALS
            )
            is_rfp_phase = any(x in narrative for x in _RFP_LITERALS)
            is_exec_escalation = (
                "exec escalation" in narrative
                or "executive escalation" in narrative
                or "escalated to" in narrative
                or bool(re.search(r"\bebc\b", narrative))
            )

            return {
                "close_date": close_date,
                "is_past_close_date": is_past,
                "competitor_confirmed_final": competitor_final,
                "competitor_name": competitor_name,
                "competitor_under_evaluation": comp_eval,
                "is_macro_ma": is_ma,
                "is_actionable_service_risk": is_service_risk,
                "is_miscellaneous_non_renewal": is_misc,
                "is_kmod_only": is_kmod,
                "is_aovpp": is_aovpp,
                "executive_skeptical": exec_skeptical,
                "risk_detail": risk_detail or narrative[:500],
                "is_already_migrating": is_already_migrating,
                "is_rfp": is_rfp_phase,
                "is_red_account": _truthy_red_ac_flag(raw_red),
                "burn_rate": br_sheet,
                "is_exec_escalation": is_exec_escalation,
            }
        except Exception as e:
            logger.warning("CRM data fetch failed for opp %s: %s", opp_id, e)
            return _sheet_only_base()

    def _extract_competitor(self, text: str) -> str:
        for pattern in (
            r"signed with ([\w\s]+?)(?:\.|,|$)",
            r"migrated to ([\w\s]+?)(?:\.|,|$)",
        ):
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip().title()
        return ""
