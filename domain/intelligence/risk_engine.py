"""
domain/intelligence/risk_engine.py
AI-powered risk analysis — March 30-style themes + LLM prompts, plus RiskEngine for workflows.
"""
from typing import Any, Dict

from log_utils import log_debug

# ---------------------------------------------------------------------------
# Risk themes → playbook recommendations (display names as keys)
# ---------------------------------------------------------------------------

RISK_RECOMMENDATION_MAP = {
    "Platform Underutilization": {
        "recommendations": [
            "Involve Consumption Leads to validate metrics and address underutilization",
            "AOVPP Swaps: Move unused GMV into Agentforce, Retail Cloud, or Marketing Cloud",
            "Right-size the contract at renewal",
        ],
    },
    "Financial & Contractual": {
        "recommendations": [
            "Executive Outreach: B2C roadmap sessions showcasing Agentic Commerce",
            "Commercial Leverage: Multi-cloud deals, revisit GMV pricing, AOVPP restructure",
        ],
    },
    "Competitive Threat": {
        "recommendations": [
            "Executive Business Review with CIO and digital leadership",
            "Demonstrate value delivered vs competitor TCO analysis",
        ],
    },
    "Technical Challenges": {
        "recommendations": [
            "Engage Product Engineering for architecture review",
            "Accelerate feature roadmap alignment",
        ],
    },
    "Business Model Change": {
        "recommendations": [
            "Strategic account planning with executive sponsors",
            "Explore platform expansion opportunities",
        ],
    },
}

NON_ACTIONABLE_REASONS = {
    "Competitive",
    "Business Change & Distress",
    "Merger & Acquisition",
    "Policy & Compliance",
    "Downsizing / Reduce Spend",
}


def is_actionable(risk_reason: str) -> bool:
    if not risk_reason:
        return True
    for reason in NON_ACTIONABLE_REASONS:
        if reason.lower() in risk_reason.lower():
            return False
    return True


def classify_risk_situation(risk_reason: str, risk_detail: str, description: str) -> dict:
    """
    Classify risk into themes based on Salesforce fields.
    Returns: {"theme": str, "confidence": str}
    """
    text = f"{risk_reason} {risk_detail} {description}".lower()

    if any(kw in text for kw in ["underutilization", "low usage", "not using", "gmv", "utilization"]):
        return {"theme": "Platform Underutilization", "confidence": "high"}

    if any(kw in text for kw in ["budget", "cost", "pricing", "contract", "renegotiate", "discount"]):
        return {"theme": "Financial & Contractual", "confidence": "high"}

    if any(kw in text for kw in ["competitor", "alternative", "evaluating", "considering", "switch"]):
        return {"theme": "Competitive Threat", "confidence": "high"}

    if any(kw in text for kw in ["technical", "performance", "integration", "bug", "issue"]):
        return {"theme": "Technical Challenges", "confidence": "medium"}

    if any(kw in text for kw in ["pivot", "restructure", "acquisition", "merger", "strategy change"]):
        return {"theme": "Business Model Change", "confidence": "medium"}

    return {"theme": "Platform Underutilization", "confidence": "low"}


def classify_risk(
    risk_reason: str,
    risk_detail: str = "",
    description: str = "",
    utilization: str = "",
) -> dict:
    """
    Classify risk for workflows; same themes as classify_risk_situation, shape expected by RiskEngine.
    """
    try:
        util_val = float(str(utilization).replace("%", "").strip())
        util_hint = " low utilization" if util_val < 50 else ""
    except (TypeError, ValueError):
        util_hint = ""

    cls = classify_risk_situation(risk_reason, risk_detail, f"{description}{util_hint}")
    theme_name = cls["theme"]
    entry = RISK_RECOMMENDATION_MAP.get(
        theme_name,
        RISK_RECOMMENDATION_MAP["Platform Underutilization"],
    )
    recs = list(entry.get("recommendations", []))
    conf_raw = cls["confidence"]
    confidence = conf_raw[:1].upper() + conf_raw[1:] if conf_raw else "Low"

    return {
        "theme": theme_name,
        "description": theme_name,
        "recommendations": recs,
        "examples": [],
        "confidence": confidence,
        "actionable": is_actionable(risk_reason),
    }


def _forecasted_atr_amount(opp: dict) -> float:
    raw = opp.get("Forecasted_Attrition__c") if opp else None
    if raw is None:
        return 0.0
    try:
        return abs(float(raw))
    except (TypeError, ValueError):
        return 0.0


def _enrichment_slices(snowflake_enrichment: dict | None) -> tuple[dict, dict, dict, str]:
    """Normalize enrich_account-shaped vs workflow-shaped analytics payloads."""
    if not snowflake_enrichment:
        return {}, {}, {}, ""

    e = snowflake_enrichment
    if "ari" in e or "health" in e or "renewal_aov" in e:
        return (
            e.get("ari", {}),
            e.get("health", {}),
            e.get("usage", {}),
            "",
        )

    usage = e.get("usage") or {}
    lines = []
    for s in (e.get("ari_scores") or [])[:5]:
        prod = s.get("product") or "Portfolio"
        cat = s.get("category") or "N/A"
        prob = s.get("probability")
        lines.append(f"- {prod}: {cat}" + (f" (prob {prob})" if prob is not None else ""))
    extra = "\n".join(lines)
    return {}, {}, usage, extra


def generate_risk_analysis(
    account_name: str,
    opp: dict | None = None,
    red_account: dict | None = None,
    snowflake_enrichment: dict | None = None,
    call_llm_fn=None,
) -> tuple[str, str]:
    """Generate risk notes + recommendations via a single structured LLM call."""
    from domain.analytics.snowflake_client import format_enrichment_for_claude

    opp = opp or {}
    red_account = red_account or {}
    enrichment_text = format_enrichment_for_claude(snowflake_enrichment or {})
    opp_stage = opp.get("StageName", "N/A")
    opp_atr = opp.get("Forecasted_Attrition__c", "N/A")
    red_reason = red_account.get("Reason__c", "") or ""

    prompt = f"""You are a Salesforce renewal risk analyst. Analyze this account and respond in EXACTLY this format:

RISK_NOTES:
<2-3 bullet points on key risks>

RECOMMENDATION:
<1-2 bullet points on recommended actions>

Account: {account_name}
Stage: {opp_stage}
Forecasted ATR: {opp_atr}
Red Account Reason: {red_reason}
{enrichment_text}"""

    system_prompt = (
        "You are a concise Salesforce renewal analyst. "
        "Always respond with exactly two sections: RISK_NOTES: and RECOMMENDATION:"
    )

    if not call_llm_fn:
        return "Data unavailable", "Review manually"

    try:
        raw = call_llm_fn(prompt, system_prompt=system_prompt, max_tokens=500)
        if not raw:
            return "Data unavailable", "Review manually"

        risk_notes, recommendation = "N/A", "N/A"
        if "RISK_NOTES:" in raw and "RECOMMENDATION:" in raw:
            parts = raw.split("RECOMMENDATION:", 1)
            risk_notes = parts[0].replace("RISK_NOTES:", "").strip()
            recommendation = parts[1].strip()
        else:
            risk_notes = raw.strip()

        return risk_notes, recommendation
    except Exception as e:
        log_debug(f"Combined risk analysis LLM error: {str(e)[:100]}")
        return f"Risk analysis unavailable: {str(e)[:60]}", "Review manually"


class RiskEngine:
    """Structured risk + adoption analysis for workflow services."""

    def __init__(self, call_llm_fn=None):
        self.call_llm_fn = call_llm_fn

    def _enrichment_from_analytics(self, analytics: Dict[str, Any]) -> Dict[str, Any]:
        usage = analytics.get("usage") or {}
        ari = analytics.get("ari_score")
        attrition = analytics.get("attrition") or {}
        products = attrition.get("products") or []
        ari_scores = []
        for p in products[:15]:
            ari_scores.append({
                "product": p.get("product"),
                "category": p.get("category"),
                "probability": None,
                "reason": "",
            })
        if ari is not None:
            if not any(s.get("probability") for s in ari_scores):
                ari_scores.insert(0, {
                    "product": "Portfolio",
                    "category": "N/A",
                    "probability": float(ari),
                    "reason": "Snowflake aggregate ARI",
                })
        util_fmt: Dict[str, Any] = {}
        utilization = usage.get("utilization_rate")
        if utilization is not None:
            util_fmt["utilization_rate"] = (
                utilization if isinstance(utilization, str) else f"{float(utilization):.1f}%"
            )
        gmv = usage.get("gmv_rate")
        if gmv is not None:
            util_fmt["gmv_rate"] = gmv if isinstance(gmv, str) else f"{float(gmv):.1f}%"
        burn = usage.get("burn_rate")
        if burn is not None:
            util_fmt["burn_rate"] = burn if isinstance(burn, str) else f"{float(burn):.1f}%"
        cc_aov = usage.get("cc_aov")
        if cc_aov is not None:
            util_fmt["cloud_aov"] = cc_aov if isinstance(cc_aov, str) else f"${float(cc_aov):,.0f}"
        if usage.get("territory"):
            util_fmt["territory"] = usage.get("territory")
        if usage.get("csg_geo"):
            util_fmt["csg_geo"] = usage.get("csg_geo")

        return {"ari_scores": ari_scores, "usage": util_fmt, "degraded": []}

    def analyze_risk(self, account_data: Dict[str, Any]) -> Dict[str, Any]:
        sf = account_data.get("salesforce", {})
        acc = sf.get("account") or {}
        red = sf.get("red_account")
        account_name = acc.get("Name") or str(account_data.get("account_id", "Account"))
        analytics = account_data.get("analytics", {})
        enr = self._enrichment_from_analytics(analytics)

        risk_reason = "N/A"
        risk_detail = ""
        if red:
            risk_reason = str(red.get("Issue_Product__c") or red.get("Stage__c") or "Red Account")
            risk_detail = str(red.get("Latest_Updates__c") or red.get("Action_Plan__c") or "")[:500]

        utilization = str((enr.get("usage") or {}).get("utilization_rate") or "")
        classification = classify_risk(
            risk_reason,
            risk_detail,
            str(acc.get("Industry") or acc.get("Type") or ""),
            utilization,
        )

        risk_notes, recommendation = generate_risk_analysis(
            account_name,
            None,
            red,
            enr,
            self.call_llm_fn,
        )

        top_cat = None
        for s in enr.get("ari_scores") or []:
            if s.get("category") and str(s.get("category")).upper() != "N/A":
                top_cat = s.get("category")
                break
        if top_cat is None and enr.get("ari_scores"):
            top_cat = (enr["ari_scores"][0] or {}).get("category")

        return {
            "summary": classification["theme"],
            "risk_notes": risk_notes,
            "recommendation": recommendation,
            "ari_category": top_cat,
            "ari_probability": analytics.get("ari_score"),
            "confidence": classification["confidence"],
            "license_at_risk_reason": risk_reason if risk_reason != "N/A" else None,
        }

    def generate_adoption_pov(self, account_data: Dict[str, Any]) -> Dict[str, Any]:
        analytics = account_data.get("analytics", {})
        usage = analytics.get("usage") or {}
        attrition = analytics.get("attrition") or {}
        products = attrition.get("products") or []

        def fmt_pct(v):
            if v is None:
                return None
            if isinstance(v, str):
                return v
            return f"{float(v):.1f}%"

        def fmt_money(v):
            if v is None:
                return None
            if isinstance(v, str):
                return v
            return f"${float(v):,.0f}"

        narrative_parts = []
        if products:
            narrative_parts.append(
                f"Snowflake attrition signals: **{len(products)}** product row(s) on latest snapshot."
            )

        return {
            "utilization_rate": fmt_pct(usage.get("utilization_rate")),
            "gmv_rate": fmt_pct(usage.get("gmv_rate")),
            "burn_rate": fmt_pct(usage.get("burn_rate")),
            "cc_aov": fmt_money(usage.get("cc_aov")),
            "territory": usage.get("territory"),
            "csg_geo": usage.get("csg_geo"),
            "narrative": "\n".join(narrative_parts) if narrative_parts else None,
        }
