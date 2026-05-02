"""
Bulk GM Review workflow.
Fetches all data in 3 queries, joins in memory.
"""
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional

from domain.integrations.gsheet_exporter import (
    apply_classification_dropdown,
    batch_write_classifications,
)

from domain.analytics.bulk_cidm import (
    get_blaze_cc_aov_by_combo_bulk,
    get_commerce_burn_rate_bulk,
    get_usage_bulk,
)
from domain.analytics.bulk_renewals import get_atrisk_renewals_bulk
from domain.intelligence.risk_engine import build_why_explanation
from log_utils import log_debug
from domain.salesforce.bulk_org62 import (
    get_opp_dynamic_fields_bulk,
    get_red_accounts_bulk,
)
from services.classify_renewal_workflow import ClassifyRenewalWorkflow
from services.gm_review_workflow import GMReviewWorkflow

logger = logging.getLogger(__name__)


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _map_canvas_review_to_bulk_row(r: dict, cloud: str) -> dict:
    opp = r.get("opp") or {}
    display = r.get("snowflake_display") or {}
    enrichment = r.get("enrichment") or {}
    renewal_aov = enrichment.get("renewal_aov") or {}
    red = r.get("red_account") or {}

    atr_snow = display.get("renewal_atr") or renewal_aov.get("renewal_atr_snow")
    atr = abs(_to_float(
        r.get("forecasted_atr")
        or atr_snow
        or 0
    ))

    close_date = str(opp.get("CloseDate") or "")
    opportunity_id = str(r.get("opportunity_id") or opp.get("Id") or "")

    return {
        "account": r.get("account_name") or "",
        "account_id": r.get("account_id") or "",
        "opportunity_id": opportunity_id,
        "cloud": r.get("cloud") or cloud,
        "cc_aov": _to_float(
            renewal_aov.get("renewal_aov")
            or renewal_aov.get("cc_aov")
            or display.get("renewal_aov")
            or 0
        ),
        "atr": atr,
        "forecasted_attrition": atr,
        "territory": (
            display.get("csg_territory")
            or renewal_aov.get("csg_territory")
            or "Unknown"
        ),
        "close_date": close_date,
        "fiscal_year": str(r.get("fiscal_year") or ""),
        "stage": opp.get("StageName") or "Unknown",
        "renewal_status": renewal_aov.get("renewal_status") or opp.get("StageName") or "Unknown",
        "utilization_rate": display.get("utilization_rate") or "N/A",
        "gmv_rate": "N/A",
        "sf_products": "",
        "risk_category": display.get("ari_category") or "Unknown",
        "risk_detail": display.get("ari_reason") or r.get("risk_notes") or "",
        "red_notes": red.get("Latest_Updates__c") or red.get("latest_updates") or "",
        "days_red": red.get("Days_Red__c") or red.get("days_red") or 0,
        "ae": renewal_aov.get("ae_name") or ((opp.get("Owner") or {}).get("Name") or ""),
        "renewal_manager": renewal_aov.get("renewal_manager") or "Unknown",
        "csm": renewal_aov.get("csm_name") or "Unknown",
        "swing": _to_float(
            r.get("conv_swing_amt")
            or display.get("swing")
            or renewal_aov.get("renewal_swing_snow")
            or 0
        ),
        "next_steps": opp.get("Next_Steps__c") or opp.get("NextStep") or "",
        "manager_notes": opp.get("Manager_Notes__c") or opp.get("PAM_Comment__c") or "",
        "slack_channel": "",
    }


def _derive_lifecycle_stage(row: dict) -> str:
    """
    Derive lifecycle stage from available signals.
    New -> Activated -> Expanding -> At-Risk -> Dormant
    """
    atr = abs(float(row.get("atr") or 0))
    
    # Safe util parsing — handle N/A, empty, None
    util_raw = str(row.get("utilization_rate") or "0").replace("%", "").strip()
    try:
        util = float(util_raw) if util_raw and util_raw != "N/A" else 0.0
    except ValueError:
        util = 0.0
    days_red = int(row.get("days_red") or 0)
    red_notes = row.get("red_notes") or ""

    # Dormant: very low utilization + no red activity
    if util < 1.0 and not red_notes and days_red == 0:
        return "Dormant"

    # At-Risk: negative ATR or red flagged
    if atr >= 500000 or days_red > 0 or red_notes:
        return "At-Risk"

    # Expanding: high utilization
    if util > 70:
        return "Expanding"

    # Activated: some utilization
    if util > 10:
        return "Activated"

    return "New"


def run_bulk_gm_review(
    cloud: str = "Commerce Cloud",
    fy: str = None,
    opp_ids: list[str] | None = None,
    min_attrition: float = 500000,
    limit: int = 500,
) -> list[dict]:
    log_debug(f"Bulk GM Review: cloud={cloud}")
    explicit_opp_ids = list(opp_ids or [])

    # Step 1: renewals
    log_debug("Step 1: Fetching renewals...")
    renewals = get_atrisk_renewals_bulk(
        cloud,
        fy,
        opp_ids=explicit_opp_ids,
        min_attrition=min_attrition,
        limit=limit,
    )
    log_debug(f"  -> {len(renewals)} renewals")
    if not renewals:
        if explicit_opp_ids and len(renewals) == 0:
            log_debug("Bulk: triggering org62 fallback for explicit opp IDs")
            # Explicit opp IDs can be missing in renewal snapshots; reuse the canvas path.
            log_debug(
                "Bulk: Snowflake returned 0 rows for explicit IDs — "
                "falling back to GMReviewWorkflow"
            )
            workflow = GMReviewWorkflow(call_llm_fn=lambda *_args, **_kwargs: "")
            result = workflow.run(account_inputs=explicit_opp_ids, cloud=cloud)
            canvas_reviews = result.get("canvas_reviews") or []
            return [_map_canvas_review_to_bulk_row(r, cloud) for r in canvas_reviews]
        log_debug(
            f"Bulk: no fallback triggered — opty_ids={bool(explicit_opp_ids)}, "
            f"renewals={len(renewals)}"
        )
        return []

    # Step 2: CIDM usage
    from domain.analytics.snowflake_client import (
        fmt_amount,
    )
    from domain.content.canvas_builder import build_adoption_pov

    account_ids = list(set(r["account_id"] for r in renewals))
    log_debug(f"Step 2: Fetching CIDM usage for {len(account_ids)} accounts...")
    usage_map = get_usage_bulk(account_ids, cloud)
    log_debug(f"  -> {len(usage_map)} accounts with usage data")
    # Step 3: red accounts + org62 dynamic fields in parallel
    log_debug("Step 3: Fetching red accounts + org62 dynamic fields...")
    opp_ids = [r["opp_id_18"] for r in renewals if r.get("opp_id_18")]
    with ThreadPoolExecutor(max_workers=2) as ex:
        fut_red = ex.submit(get_red_accounts_bulk, account_ids)
        fut_org62 = ex.submit(get_opp_dynamic_fields_bulk, opp_ids)
        red_map = fut_red.result()
        org62_map = fut_org62.result()
    log_debug(f"  -> {len(red_map)} red accounts, {len(org62_map)} org62 opps")

    blaze_cc_aov_by_combo: dict[str, float] = {}
    burn_rate_map: dict[str, dict] = {}
    if renewals and "commerce" in (cloud or "").lower():
        combo_keys = [
            str(x.get("combo_company_id") or "").strip()
            for x in renewals
            if str(x.get("combo_company_id") or "").strip()
        ]
        if combo_keys:
            blaze_cc_aov_by_combo = get_blaze_cc_aov_by_combo_bulk(combo_keys)

        contract_dates: dict[str, dict] = {}
        for x in renewals:
            aid = str(x.get("account_id") or "").strip()[:15]
            if not aid:
                continue
            opp_15 = (
                str(x.get("opp_id_18") or "").strip()[:15]
                if x.get("opp_id_18")
                else ""
            )
            org62_row = org62_map.get(opp_15, {}) if opp_15 else {}
            contract_end = x.get("renewal_contract_end_date") or x.get("close_date")
            contract_start = org62_row.get("prior_contract_start_date")
            tm_raw = org62_row.get("prior_contract_term_months")
            try:
                term_months = int(float(tm_raw)) if tm_raw not in (None, "", False) else 12
            except (TypeError, ValueError):
                term_months = 12
            contract_dates[aid] = {
                "renewal_close_dt": str(contract_end or "").strip(),
                "contract_start_date": contract_start,
                "term_months": max(term_months, 1),
            }
        burn_ids = list(contract_dates.keys())
        if burn_ids:
            burn_rate_map = get_commerce_burn_rate_bulk(burn_ids, contract_dates)

    # Step 4: join
    log_debug("Step 4: Joining data...")
    rows = []
    for r in renewals:
        acct_id_15 = str(r.get("account_id") or "").strip()[:15]
        opp_id_15 = (
            str(r.get("opp_id_18") or "").strip()[:15]
            if r.get("opp_id_18")
            else ""
        )
        usage = usage_map.get(acct_id_15, {})
        org62 = org62_map.get(opp_id_15, {})
        usage_rows = (
            usage.get("raw_rows")
            or usage.get("rows")
            or usage.get("raw")
            or []
        )
        all_usage_rows = usage.get("all_raw_rows") or usage_rows
        apm_l1_products = list(dict.fromkeys(
            str(row.get("DRVD_APM_LVL_1") or "").strip()
            for row in all_usage_rows
            if str(row.get("DRVD_APM_LVL_1") or "").strip()
            and str(row.get("DRVD_APM_LVL_1") or "").strip() not in ("Other", "")
        ))
        sf_products = ", ".join(apm_l1_products) if apm_l1_products else "N/A"
        cid = str(r.get("combo_company_id") or "").strip()
        if cid and cid in blaze_cc_aov_by_combo:
            cc_aov_raw = float(blaze_cc_aov_by_combo[cid])
        else:
            cc_aov_raw = _to_float(r.get("cc_aov") or 0)

        burn_data = burn_rate_map.get(acct_id_15, {})

        row = {
            "account": r["account_name"],
            "account_id": r["account_id"],
            "opportunity_id": r["opp_id_18"],
            "cloud": r["cloud"],
            "cc_aov": cc_aov_raw,
            "atr": r["atr"],
            "forecasted_atr": r.get("forecasted_atr"),
            "forecasted_attrition": abs(_to_float(
                r.get("forecasted_atr")
                or r.get("atr")
                or 0
            )),
            "territory": r["territory"],
            "close_date": r["close_date"],
            "fiscal_year": r["fiscal_year"],
            "stage": r["stage"],
            "renewal_status": r["renewal_status"],
            "utilization_rate": usage.get("utilization_rate", "N/A"),
            "burn_rate": (
                burn_data.get("burn_rate_overall")
                if burn_data.get("burn_rate_overall") is not None
                else "N/A"
            ),
            "predicted_eoc_util": (
                burn_data.get("predicted_eoc_util")
                if burn_data.get("predicted_eoc_util") is not None
                else "N/A"
            ),
            "burn_rate_by_l2": burn_data.get("burn_rate_by_l2") or {},
            "burn_rate_status": burn_data.get("status") or "N/A",
            "burn_util_pct_overall": burn_data.get("util_pct_overall"),
            "burn_time_elapsed_pct": burn_data.get("time_elapsed_pct"),
            "gmv_rate": "N/A",
            "sf_products": sf_products,
            "risk_category": r["risk_category"],
            "risk_detail": r["risk_detail"],
            "ae": r["ae"],
            "renewal_manager": r["renewal_manager"],
            "csm": r["csm"],
            "swing": _to_float(r.get("conv_swing_amt") or 0),
            "next_steps": r.get("next_steps") or "",
            "manager_notes": r.get("manager_notes") or "",
            "latest_commentary": org62.get("description") or "",
            "adoption_pov": build_adoption_pov(usage_rows, cloud=cloud),
            "slack_channel": r["slack_channel"],
        }
        # Org62 red map (same 15-char key as get_red_accounts_bulk) — after renewal row so
        # Snowflake placeholders on r (e.g. red_notes/days_red) never shadow Org62.
        red = red_map.get(acct_id_15, {})
        if red:
            log_debug(
                f"red_ac_flag for {row.get('account', r.get('account_name', '?'))}: "
                f"{red.get('red_account_url')}"
            )
        row["red_notes"] = str(
            red.get("latest_updates") or red.get("Latest_Updates__c") or ""
        )
        try:
            row["days_red"] = int(red.get("days_red") or red.get("Days_Red__c") or 0)
        except (TypeError, ValueError):
            row["days_red"] = 0
        row["red_ac_flag"] = str(red.get("red_account_url") or "").strip()
        row["red_issue_product"] = str(
            red.get("issue_product") or red.get("Issue_Product__c") or ""
        ).strip()

        row["lifecycle_stage"] = _derive_lifecycle_stage(row)
        row["why_explanation"] = build_why_explanation(
            account=row.get("account", ""),
            atr=abs(float(row.get("atr") or 0)),
            risk_theme=row.get("risk_theme") or row.get("risk_category") or "Unspecified",
            risk_notes=row.get("risk_notes") or "",
            utilization_rate=row.get("utilization_rate") or "0%",
            days_red=int(row.get("days_red") or 0),
            close_date=str(row.get("close_date") or ""),
        )
        rows.append(row)

    # Safety cap
    MAX_ROWS = int(os.getenv("GM_REVIEW_MAX_ROWS", "1000"))
    if len(rows) > MAX_ROWS:
        log_debug(f"  ⚠️  Capping at {MAX_ROWS} rows (GM_REVIEW_MAX_ROWS)")
        rows = rows[:MAX_ROWS]

    log_debug(f"  -> {len(rows)} rows ready")
    return rows


def _lookup_record_channel(account_id: str | None) -> Optional[str]:
    """Resolve Salesforce Account Id → Slack record channel id, if wired."""
    if not account_id:
        return None
    try:
        from domain.integrations.salesforce_channels import lookup_record_channel

        return lookup_record_channel(account_id)
    except Exception:
        return None


def _run_classification_pass(
    slack_client: Any,
    org62_client: Any,
    worksheet,
    exported_rows: list[dict],
    *,
    start_row_index: int,
) -> None:
    """
    Stage 2 — auto-classify exported rows and write column AA after ``append_rows``.

    Rows are keyed to sheet lines using ``start_row_index`` for the first exported row.
    """
    workflow = ClassifyRenewalWorkflow(
        slack_client=slack_client,
        org62_client=org62_client,
    )

    try:
        apply_classification_dropdown(worksheet)
    except Exception as e:
        logger.warning("[Stage 2] Dropdown attach failed (non-fatal): %s", e)

    classifications: list[str] = []
    for offset, row in enumerate(exported_rows):
        sheet_row_index = start_row_index + offset
        try:
            record_channel_id = _lookup_record_channel(row.get("account_id"))
            result = workflow.classify(row, record_channel_id)
            classifications.append(result.recommendation)
            logger.info(
                "[Stage 2] %s → %s (%s)",
                row.get("account_nm"),
                result.recommendation,
                result.rule_applied,
            )
        except Exception as e:
            logger.warning(
                "[Stage 2] Classification failed for %s row %s: %s",
                row.get("account_nm", "?"),
                sheet_row_index,
                e,
            )
            classifications.append("Pending Review")

    try:
        batch_write_classifications(
            worksheet, start_row_index, classifications
        )
    except Exception as ee:
        logger.warning(
            "[Stage 2] Batch write classifications failed (%s rows): %s",
            len(classifications),
            ee,
        )
