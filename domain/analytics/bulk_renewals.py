"""
Bulk Snowflake RENEWALS queries.
Single query returns ALL at-risk renewals for a cloud.
"""
import os
import datetime

from log_utils import log_debug
from domain.analytics.snowflake_client import run_query


# Stages excluded from bulk at-risk pull (closed / dead / courtesy).
_RENEWAL_STG_EXCLUDED = (
    "Dead Attrition",
    "05 Closed",
    "Dead - Duplicate",
    "Dead - No Decision",
    "Dead - No Opportunity",
    "NP - Dead Duplicate",
    "08 - Closed",
    "Closed",
    "Closed and referral paid",
    "Loss - Off Contract",
    "UNKNOWN",
    "Courtesy",
)


def _renewal_stg_not_in_sql() -> str:
    escaped = "', '".join(s.replace("'", "''") for s in _RENEWAL_STG_EXCLUDED)
    return f"AND RENEWAL_STG_NM NOT IN ('{escaped}')"


# Three-tier view strategy
_PRIMARY_VIEW = "SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW"
_FALLBACK_VIEW = "SSE_DM_CSG_RPT_PRD.RENEWALS.CI_NEAR_REALTIME_RENEWAL_OPTY_VW"
_SNAP_VIEW = "SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_SNAP_VW"


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _build_renewal_query(
    view: str,
    cloud_filter: str,
    fy_filter: str,
    fy_lookahead_filter: str,
    atr_filter: str,
    opp_filter: str,
    stg_excl: str,
    limit: int = 500,
) -> str:
    red_green_select = "RED_GREEN," if view != _SNAP_VIEW else ""
    red_green_filter = " OR (RED_GREEN = 'Red')" if view != _SNAP_VIEW else ""
    atr_expr = (atr_filter or "").strip()
    if atr_expr.upper().startswith("AND "):
        atr_expr = atr_expr[4:].strip()

    # For explicit opp IDs, do not apply risk/red filters.
    if opp_filter:
        risk_and_red_filter = ""
    else:
        risk_and_red_filter = f"AND (({atr_expr}){red_green_filter})"

    return f"""
        SELECT
            RENEWAL_OPTY_ID,
            RENEWAL_OPTY_ID_18,
            ACCT_ID,
            ACCOUNT_18_ID,
            ACCOUNT_NM,
            COALESCE(RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV, RENEWAL_AMT_CONV, 0) AS RENEWAL_AOV,
            RENEWAL_FCAST_ATTRITION_CONV,
            RENEWAL_ATR_CONV,
            CONV_SWING_AMT                            AS SWING,
            RENEWAL_FCAST_CODE                        AS FCAST_CODE,
            RENEWAL_STG_NM                            AS STAGE,
            RENEWAL_STATUS,
            RENEWAL_KEY_RISK_CAT,
            RENEWAL_RISK_DETAIL,
            RENEWAL_CLOSE_MONTH,
            RENEWAL_CLSD_DT,
            RENEWAL_FISCAL_YEAR,
            CSG_TERRITORY,
            CSG_AREA,
            CSG_GEO,
            TARGET_CLOUD,
            {red_green_select}
            AE_FULL_NM                                AS AE,
            RENEWAL_OPTY_OWNR_NM                      AS RENEWAL_MANAGER,
            ACCT_CSM                                  AS CSM,
            NEXT_STEPS,
            MANAGER_NOTES,
            MANAGER_FORECAST_JUDGEMENT,
            DRVD_BU
        FROM {view}
        WHERE CURR_SNAP = 'Y'
        {stg_excl}
        {cloud_filter}
        {fy_filter}
        {fy_lookahead_filter}
        {opp_filter}
        {risk_and_red_filter}
        AND (RENEWAL_CLSD_DT >= CURRENT_DATE OR RENEWAL_CLSD_DT IS NULL)
        ORDER BY RENEWAL_CLSD_DT ASC NULLS LAST,
                 RENEWAL_FCAST_ATTRITION_CONV ASC NULLS LAST
        LIMIT {limit}
    """


def _map_renewal_rows(rows: list[dict]) -> list[dict]:
    """Normalize raw Snowflake renewal rows to bulk row schema."""
    out = []
    for r in rows:
        fcast = r.get("RENEWAL_FCAST_ATTRITION_CONV")
        atr = r.get("RENEWAL_ATR_CONV")
        out.append({
            "opp_id": r.get("RENEWAL_OPTY_ID", ""),
            "opp_id_18": r.get("RENEWAL_OPTY_ID_18", ""),
            "account_id": r.get("ACCT_ID") or r.get("ACCOUNT_18_ID", ""),
            "account_18_id": r.get("ACCOUNT_18_ID", ""),
            "account_name": r.get("ACCOUNT_NM", ""),
            "cloud": r.get("TARGET_CLOUD", ""),
            "cc_aov": float(r.get("RENEWAL_AOV") or r.get("RENEWAL_AMT_CONV") or 0),
            "atr": abs(float(atr or 0)),
            "forecasted_atr": abs(float(fcast or atr or 0)),
            "swing": abs(float(r.get("SWING") or 0)),
            "renewal_status": r.get("RENEWAL_STATUS") or r.get("FCAST_CODE") or r.get("STAGE") or "",
            "stage": r.get("STAGE") or "",
            "close_date": str(r.get("RENEWAL_CLSD_DT") or ""),
            "renewal_month": str(r.get("RENEWAL_CLOSE_MONTH") or ""),
            "fiscal_year": str(r.get("RENEWAL_FISCAL_YEAR") or ""),
            "territory": r.get("CSG_TERRITORY") or r.get("CSG_AREA") or "",
            "csg_geo": r.get("CSG_GEO") or "",
            "target_cloud": r.get("TARGET_CLOUD") or "",
            "ae": r.get("AE") or "",
            "renewal_manager": r.get("RENEWAL_MANAGER") or "",
            "csm": r.get("CSM") or "",
            "next_steps": r.get("NEXT_STEPS") or "",
            "manager_notes": r.get("MANAGER_NOTES") or "",
            "manager_forecast": r.get("MANAGER_FORECAST_JUDGEMENT") or "",
            "risk_category": r.get("RENEWAL_KEY_RISK_CAT") or "",
            "risk_detail": r.get("RENEWAL_RISK_DETAIL") or "",
            "drvd_bu": r.get("DRVD_BU") or "",
            "slack_channel": "",
            "utilization_rate": "N/A",
            "red_notes": "",
            "days_red": "",
        })
    return out


def get_atrisk_renewals_bulk(
    cloud: str,
    fy: str = None,
    opp_ids: list[str] | None = None,
    min_attrition: float = 500000,
    limit: int = 500,
) -> list[dict]:
    """
    Single query -> ALL at-risk renewals for a cloud.
    RENEWAL_FISCAL_YEAR is numeric (e.g. 2027), not 'FY2027'.
    """
    stg_excl = _renewal_stg_not_in_sql()
    opty_ids = list(opp_ids or [])
    if opty_ids:
        ids_sql = ", ".join(f"'{i.strip()}'" for i in opty_ids)
        opp_filter = f"AND (RENEWAL_OPTY_ID IN ({ids_sql}) OR RENEWAL_OPTY_ID_18 IN ({ids_sql}))"
        cloud_filter = ""
        fy_filter = ""
        fy_lookahead_filter = ""
        atr_filter = ""
    else:
        opp_filter = ""
        cloud_filter = _build_cloud_filter(cloud) if cloud else ""
        fy_filter = ""
        if fy:
            fy_val = str(fy).strip()
            fy_numeric = int(fy_val.replace("FY", "").replace("fy", ""))
            fy_filter = f"AND RENEWAL_FISCAL_YEAR = {fy_numeric}"
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().month
        # Salesforce FY starts Feb 1, so month>=2 maps to next fiscal year label.
        current_fy = current_year + 1 if current_month >= 2 else current_year
        max_fy = current_fy + 1
        fy_lookahead_filter = f"AND RENEWAL_FISCAL_YEAR <= {max_fy}"
        # Same threshold for all clouds
        threshold = float(os.getenv("GM_REVIEW_FCAST_ATTRITION_THRESHOLD", "-500000"))
        atr_filter = f"AND RENEWAL_FCAST_ATTRITION_CONV <= {threshold}"

    def _query(view: str) -> list[dict]:
        q = _build_renewal_query(
            view,
            cloud_filter,
            fy_filter,
            fy_lookahead_filter,
            atr_filter,
            opp_filter,
            stg_excl,
            limit=limit,
        )
        if view != _SNAP_VIEW:
            q = q.replace("WHERE CURR_SNAP = 'Y'", "WHERE 1=1")
        try:
            rows = run_query(
                q,
                [],
                statement_timeout=_env_int("SNOWFLAKE_RENEWAL_STATEMENT_TIMEOUT", 120),
            )
            return _map_renewal_rows(rows)
        except Exception as e:
            log_debug(f"get_atrisk_renewals_bulk [{view.split('.')[-1]}]: {str(e)[:100]}")
            return []

    rows = _query(_PRIMARY_VIEW)
    if rows:
        log_debug(f"Bulk: {len(rows)} rows from CI_NEAR_REALTIME_RENEWAL_OPTY_VW")
        return rows

    log_debug("Bulk: primary view empty — trying WV_CI_RENEWAL_OPTY_VW")
    rows = _query(_FALLBACK_VIEW)
    if rows:
        log_debug(f"Bulk: {len(rows)} rows from WV_CI_RENEWAL_OPTY_VW")
        return rows

    log_debug("Bulk: fallback view empty — trying WV_CI_RENEWAL_OPTY_SNAP_VW")
    rows = _query(_SNAP_VIEW)
    log_debug(f"Bulk: {len(rows)} rows from WV_CI_RENEWAL_OPTY_SNAP_VW")
    return rows


def _build_cloud_filter(cloud: str) -> str:
    """Build cloud filter for RENEWALS view."""
    if not cloud:
        return ""
    c = str(cloud).strip().lower()
    if c == "all":
        return ""
    if "financial services" in c or c == "fsc":
        snapshot_date = os.getenv("SNOWFLAKE_CIDM_SNAPSHOT_DT", "2026-04-01")
        return f"""AND ACCT_ID IN (
        SELECT DISTINCT ACCOUNT_ID
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_CI_ACCT_PRODUCT_APM_VW
        WHERE APM_L3 IN (
            'Financial Services Cloud - Sales',
            'Financial Services Cloud - Service'
        )
        AND CURR_SNAP = 'Y'
    )"""
    if "commerce" in c:
        return """AND (
            TARGET_CLOUD LIKE '%Commerce%'
            OR TARGET_CLOUD LIKE '%B2C%'
            OR TARGET_CLOUD LIKE '%B2B Commerce%'
            OR TARGET_CLOUD LIKE '%Order Management%'
            OR RENEWAL_OPTY_NM LIKE '%Commerce%'
            OR RENEWAL_OPTY_NM LIKE '%B2C%'
            OR RENEWAL_OPTY_NM LIKE '%B2B%'
        )"""
    cloud_safe = str(cloud).replace("'", "''")
    return f"AND (TARGET_CLOUD LIKE '%{cloud_safe}%' OR RENEWAL_OPTY_NM LIKE '%{cloud_safe}%')"
