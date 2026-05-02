"""
Bulk Snowflake RENEWALS queries.
Single query returns ALL at-risk renewals for a cloud (parent rollup on COMBO_COMPANY_ID).
"""
import datetime
import os
import re

from log_utils import log_debug
from domain.analytics.snowflake_client import run_query


# Legacy non-commerce bulk path: excluded stage list on inner WV_CI_RENEWAL_OPTY_VW.
DEAD_STAGES = (
    "05 Closed",
    "Dead Attrition",
    "Dead - Duplicate",
    "Dead - No Decision",
    "Dead - No Opportunity",
    "NP - Dead Duplicate",
    "08 - Closed",
    "Closed",
    "Loss - Off Contract",
    "UNKNOWN",
    "Courtesy",
)


def _sql_quote_list(values: tuple[str, ...]) -> str:
    return ", ".join("'" + s.replace("'", "''") + "'" for s in values)


_DEAD_STAGE_SQL = _sql_quote_list(DEAD_STAGES)

# Wide view (same primary grain as ad-hoc renewal diagnostics)
_PRIMARY_VIEW = "SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW"
_APM_VIEW = "SSE_DM_CSG_RPT_PRD.CIDM.WV_CI_ACCT_PRODUCT_APM_VW"
_USAGE_VIEW = "SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW"


def _renewal_thirty_de_exclusion_list_sql() -> str:
    vals = list(str(i) for i in range(1, 14)) + ["i30DE"]
    return ", ".join("'" + v.replace("'", "''") + "'" for v in vals)


def _commerce_book_of_business_or_target_cloud_sql() -> str:
    """Row matches Commerce GM Review scope: pure BoB Commerce or Core/MC with commerce products."""
    return """(
                       BOOK_OF_BUSINESS = 'Commerce Cloud'
                    OR TARGET_CLOUD LIKE '%Commerce%'
                    OR TARGET_CLOUD LIKE '%B2C%'
                    OR TARGET_CLOUD LIKE '%B2B Commerce%'
                    OR TARGET_CLOUD LIKE '%Order Management%'
                  )"""


def _commerce_stage1_base_where_sql(threshold: float, clsd_months_forward: int) -> str:
    """
    Commerce bulk: applied **after** the Commerce BoB / ``TARGET_CLOUD`` row predicate on
    ``WV_CI_RENEWAL_OPTY_VW`` — ATR threshold, close-date window, 30DE carve-out, M&A flag,
    etc. ``QUALIFY`` then picks the worst **Commerce** opp per ``COMBO_COMPANY_ID``.
    """
    thirty = _renewal_thirty_de_exclusion_list_sql()
    return f"""
                  AND RENEWAL_FCAST_ATTRITION_CONV <= {threshold}
                  AND RENEWAL_FCAST_CODE NOT IN ('Omitted')
                  AND M_A_LEGACY_RENEWAL_FLAG != 'Y'
                  AND (
                       RENEWAL_OPTY_TYPE = 'Renewal'
                    OR RENEWAL_OPTY_TYPE = 'Contract'
                    OR RENEWAL_OPTY_TYPE LIKE 'Off-Cycle%'
                  )
                  AND ACCOUNT_NM NOT LIKE '%Off Cycle%'
                  AND RENEWAL_CLSD_DT >= DATE_TRUNC('month', SYSDATE())
                  AND RENEWAL_CLSD_DT <= ADD_MONTHS(DATE_TRUNC('month', SYSDATE()), {clsd_months_forward})
                  AND (
                        (
                             (RENEWAL_30DE NOT IN ({thirty}))
                         AND RENEWAL_STG_NM IN ('05 Closed','Dead Attrition'))
                    OR RENEWAL_STG_NM NOT IN ('05 Closed','Dead Attrition')
                    OR RENEWAL_30DE IS NULL
                  )"""


def _commerce_outer_include_where_sql() -> str:
    """Which accounts (rows) stay in Commerce GM Review after worst-opp pick — BoB / TARGET_CLOUD only."""
    return """(
                   BOOK_OF_BUSINESS = 'Commerce Cloud'
                OR TARGET_CLOUD LIKE '%Commerce%'
                OR TARGET_CLOUD LIKE '%B2C%'
                OR TARGET_CLOUD LIKE '%B2B Commerce%'
                OR TARGET_CLOUD LIKE '%Order Management%'
            )"""


def _commerce_query1_where_clause(threshold: float, clsd_months_forward: int) -> str:
    """
    Legacy: base + commerce row predicate (same-window single-query filter).
    Commerce bulk uses the same predicates: commerce row filter first, then
    ``_commerce_stage1_base_where_sql``, then ``QUALIFY``.
    """
    bob_tgt = _commerce_book_of_business_or_target_cloud_sql()
    return f"""
                  AND {bob_tgt.strip()}
                  {_commerce_stage1_base_where_sql(threshold, clsd_months_forward).strip()}
"""


def _build_rollup_having(
    cloud: str,
    threshold: float = -500000,
    *,
    include_fcast: bool = True,
    commerce_use_book_of_business: bool = False,
) -> str:
    """
    Rollup COMBO gate: Commerce ``BOOK_OF_BUSINESS`` sums when enabled; otherwise
    legacy commerce ``TARGET_CLOUD`` / other clouds unchanged.
    For Commerce bulk battle-tested INNER WHERE we already constrain
    ``RENEWAL_FCAST_ATTRITION_CONV``, so rollup fcast clause is skipped when BoB commerce.
    """
    fcast_clause = ""
    if include_fcast:
        fcast_clause = (
            f"\n        AND SUM(RENEWAL_FCAST_ATTRITION_CONV) <= {threshold}"
        )
    if "commerce" in (cloud or "").lower():
        if commerce_use_book_of_business:
            _bob_t = _commerce_book_of_business_or_target_cloud_sql().strip()
            return (
                f"\n        HAVING SUM(CASE WHEN {_bob_t}"
                "\n            THEN RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV"
                "\n            ELSE 0 END) > 0"
            )
        return (
            f"\n        HAVING SUM(CASE WHEN TARGET_CLOUD LIKE '%Commerce%'"
            f"\n            THEN RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV"
            f"\n            ELSE 0 END) > 0{fcast_clause}"
        )
    return f"\n        HAVING SUM(RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV) > 0{fcast_clause}"


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _alias_predicate_for_outer_renewals_join(
    predicate_sql: str, alias: str = "r2"
) -> str:
    """
    ``cloud_filter`` is written for bare ``WV_CI_RENEWAL_OPTY_VW`` columns in the inner
    subquery. The same predicates are reused on ``JOIN ... r2`` next to ``LEFT JOIN APM``;
    unqualified identifiers like ACCT_ID become ambiguous vs the APM view — qualify here.
    """
    if not predicate_sql or not str(predicate_sql).strip():
        return ""
    s = str(predicate_sql).strip()
    while s.upper().startswith("AND"):
        s = s[3:].lstrip()

    cols = (
        "RENEWAL_FCAST_ATTRITION_CONV",
        "BOOK_OF_BUSINESS",
        "M_A_LEGACY_RENEWAL_FLAG",
        "RENEWAL_CLSD_DT",
        "RENEWAL_FCAST_CODE",
        "RENEWAL_OPTY_TYPE",
        "RENEWAL_STG_NM",
        "RENEWAL_OPTY_ID_18",
        "RENEWAL_OPTY_ID",
        "TARGET_CLOUD",
        "RENEWAL_FISCAL_YEAR",
        "RENEWAL_30DE",
        "ACCOUNT_NM",
        "RENEWAL_OPTY_NM",
        "ACCT_ID",
    )
    # Longer identifiers first avoids partial-token edge cases across column names.
    for col in sorted(cols, key=len, reverse=True):
        pattern = rf"(?<![A-Za-z0-9_.]){re.escape(col)}(?![A-Za-z0-9_])"
        s = re.sub(pattern, f"{alias}.{col}", s)

    if s.startswith("("):
        return f" AND {s}"
    return f" AND ({s})"


def _defensive_get(row: dict, *candidates, default=""):
    """Snowflake may return UPPER or mixed keys depending on client."""
    for k in candidates:
        for variant in (k, k.upper() if k else k):
            if not variant:
                continue
            v = row.get(variant)
            if v is not None and v != "":
                return v
    return default


def _map_combo_rollup_to_bulk_schema(row: dict) -> dict:
    """
    Map COMBO_COMPANY_ID rollup row to the legacy per-opp bulk row shape
    (used by gm_review_bulk_workflow and app_home).
    """
    acct_18 = str(_defensive_get(row, "ACCOUNT_18_ID", default=""))
    acct_15 = str(_defensive_get(row, "ACCT_ID", default=""))
    opportunity_18 = str(
        _defensive_get(row, "RENEWAL_OPTY_ID_18", "OPPORTUNITY_ID", default="")
    )
    opp_15 = opportunity_18[:15] if len(opportunity_18) >= 15 else opportunity_18
    fcast = _defensive_get(row, "FCAST_ATTRITION", "fcast_attrition", default=0) or 0
    atr_raw = _defensive_get(row, "ATR", "atr", default=0) or 0
    fcast_f = float(fcast)
    atr_f = abs(float(atr_raw))

    return {
        "opp_id": opp_15,
        "opp_id_18": opportunity_18,
        "account_id": acct_15 or acct_18,
        "account_18_id": acct_18,
        "account_name": str(_defensive_get(row, "ACCOUNT_NM", default="")),
        "cloud": str(_defensive_get(row, "ALL_PRODUCTS", "ALL_CLOUDS", default="")),
        "cc_aov": float(_defensive_get(row, "CC_AOV", default=0) or 0),
        "atr": atr_f,
        "forecasted_atr": abs(float(fcast_f) if fcast_f else atr_f),
        "conv_swing_amt": float(
            _defensive_get(row, "CONV_SWING_AMT", "conv_swing_amt", default=0) or 0
        ),
        "renewal_status": str(_defensive_get(row, "RENEWAL_STAGE", default="")),
        "stage": str(_defensive_get(row, "RENEWAL_STAGE", default="")),
        "close_date": str(_defensive_get(row, "CLOSE_MONTH", default="")),
        "renewal_contract_end_date": str(
            _defensive_get(
                row,
                "RENEWAL_CONTRACT_END_DATE",
                "renewal_contract_end_date",
                default="",
            )
            or ""
        ),
        "renewal_month": str(_defensive_get(row, "CLOSE_MONTH", default="")),
        "fiscal_year": str(_defensive_get(row, "FISCAL_YEAR", default="")),
        "territory": str(_defensive_get(row, "CSG_TERRITORY", "CSG_GEO", default="")),
        "csg_geo": str(_defensive_get(row, "CSG_GEO", default="")),
        "target_cloud": str(_defensive_get(row, "ALL_CLOUDS", default="")),
        "ae": "",
        "renewal_manager": "",
        "csm": "",
        "next_steps": "",
        "manager_notes": "",
        "manager_forecast": "",
        "risk_category": "",
        "risk_detail": "",
        "drvd_bu": "",
        "slack_channel": "",
        "utilization_rate": str(_defensive_get(row, "UTILIZATION_RATE", default="N/A") or "N/A"),
        "red_notes": "",
        "days_red": "",
        "combo_company_id": str(_defensive_get(row, "COMBO_COMPANY_ID", default="")),
        "opp_count": int(_defensive_get(row, "OPP_COUNT", default=0) or 0),
        "account_count": int(_defensive_get(row, "ACCOUNT_COUNT", default=0) or 0),
    }


def get_atrisk_renewals_bulk(
    cloud: str = "Commerce Cloud",
    fy: str = None,
    opp_ids: list[str] | None = None,
    min_attrition: float = 500000,
    limit: int = 500,
) -> list[dict]:
    """
    Bulk renewal fetch grouped by COMBO_COMPANY_ID (parent rollup).

    **Commerce Cloud** scans (no explicit ``opp_ids``): restrict to Commerce-tagged rows
    (``BOOK_OF_BUSINESS`` / ``TARGET_CLOUD``), then apply base gates (threshold, close-date
    window, 30DE carve-out, M&A flag, etc.), then
    ``QUALIFY ROW_NUMBER() … ORDER BY RENEWAL_FCAST_ATTRITION_CONV ASC`` picks the worst
    **Commerce** renewal per ``COMBO_COMPANY_ID``. Metrics come from that row (no SUM).
    Uses **GM_REVIEW_RENEWAL_CLSD_FORWARD_MONTHS** (default **24**).

    Legacy **non‑Commerce** / explicit **opp_ids** paths keep **``RENEWAL_CLOSE_MONTH``**,
    dead-stage exclusions, rollup fcast HAVING where applicable.

    When ``GM_REVIEW_FCAST_ATTRITION_THRESHOLD`` is not set, uses
    ``-min_attrition`` (default 500000) as the opp-level cutoff.
    """
    opty_ids = [x.strip() for x in (opp_ids or []) if str(x).strip()]

    env_thr = os.getenv("GM_REVIEW_FCAST_ATTRITION_THRESHOLD")
    if env_thr is not None and str(env_thr).strip() != "":
        threshold = float(env_thr)
    else:
        threshold = -float(min_attrition)
    min_close = os.getenv("SNOWFLAKE_RENEWAL_MIN_CLOSE_MONTH", "2026-02-01").strip()
    min_esc = min_close.replace("'", "''")
    commerce_in_cloud = bool(cloud and "commerce" in cloud.lower())
    is_commerce_bulk = bool(not opty_ids and commerce_in_cloud)

    if is_commerce_bulk:
        clsd_months_fwd = _env_int(
            "GM_REVIEW_RENEWAL_CLSD_FORWARD_MONTHS", 24
        )
        stage1_base = _commerce_stage1_base_where_sql(threshold, clsd_months_fwd)
        commerce_include = _commerce_outer_include_where_sql()
        rollup_inner_sql = f"""
        FROM (
            SELECT
                COMBO_COMPANY_ID,
                ACCOUNT_NM                                     AS account_nm,
                ACCOUNT_18_ID                                  AS account_18_id,
                ACCT_ID                                        AS acct_id,
                RENEWAL_OPTY_ID_18                             AS renewal_opty_id_18,
                RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV       AS cc_aov_fallback,
                RENEWAL_AMT_CONV                               AS forecasted,
                RENEWAL_ATR_CONV                               AS atr,
                RENEWAL_FCAST_ATTRITION_CONV                   AS fcast_attrition,
                COALESCE(CONV_SWING_AMT, 0)                    AS conv_swing_amt,
                RENEWAL_CLOSE_MONTH                            AS close_month,
                RENEWAL_CONTRACT_END_DATE                      AS renewal_contract_end_date,
                RENEWAL_FISCAL_YEAR                            AS fiscal_year,
                RENEWAL_STG_NM                                 AS renewal_stage,
                CSG_GEO                                        AS csg_geo,
                CSG_TERRITORY                                  AS csg_territory,
                TARGET_CLOUD                                   AS all_clouds,
                1                                              AS opp_count,
                1                                              AS account_count
            FROM (
                SELECT
                    COMBO_COMPANY_ID,
                    ACCT_ID,
                    ACCOUNT_NM,
                    ACCOUNT_18_ID,
                    RENEWAL_OPTY_ID,
                    RENEWAL_OPTY_ID_18,
                    RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV,
                    RENEWAL_AMT_CONV,
                    RENEWAL_ATR_CONV,
                    RENEWAL_FCAST_ATTRITION_CONV,
                    CONV_SWING_AMT,
                    RENEWAL_CLOSE_MONTH,
                    RENEWAL_CONTRACT_END_DATE,
                    RENEWAL_FISCAL_YEAR,
                    RENEWAL_STG_NM,
                    CSG_GEO,
                    CSG_TERRITORY,
                    TARGET_CLOUD,
                    BOOK_OF_BUSINESS
                FROM {_PRIMARY_VIEW}
                WHERE TRUE
                AND {commerce_include}
                {stage1_base}
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY COMBO_COMPANY_ID
                    ORDER BY RENEWAL_FCAST_ATTRITION_CONV ASC
                ) = 1
            ) commerce_worst_opp
        ) rollup"""
        apm_anchor_where = f"""WHERE TRUE
                  AND {commerce_include}
                  AND RENEWAL_CLSD_DT >= DATE_TRUNC('month', SYSDATE())
                  AND RENEWAL_CLSD_DT <= ADD_MONTHS(DATE_TRUNC('month', SYSDATE()), {clsd_months_fwd})"""
        r2_anchor = f"""
            AND r2.RENEWAL_CLSD_DT >= DATE_TRUNC('month', SYSDATE())
            AND r2.RENEWAL_CLSD_DT <= ADD_MONTHS(DATE_TRUNC('month', SYSDATE()), {clsd_months_fwd})"""
    else:
        having_sql = _build_rollup_having(cloud, threshold, include_fcast=not opty_ids)

        if opty_ids:
            ids_sql = ", ".join(
                "'" + i.replace("'", "''") + "'" for i in opty_ids
            )
            cloud_filter = ""
            opp_filter = f"AND (RENEWAL_OPTY_ID IN ({ids_sql}) OR RENEWAL_OPTY_ID_18 IN ({ids_sql}))"
            fy_filter = ""
            fy_lookahead_filter = ""
        else:
            opp_filter = ""
            cloud_filter = _build_cloud_filter(cloud) if cloud else ""
            fy_filter = ""
            if fy:
                fy_val = str(fy).strip()
                fy_numeric = int(
                    fy_val.replace("FY", "").replace("fy", "")
                )
                fy_filter = f"AND RENEWAL_FISCAL_YEAR = {fy_numeric}"
            current_year = datetime.datetime.now().year
            current_month = datetime.datetime.now().month
            current_fy = (
                current_year + 1 if current_month >= 2 else current_year
            )
            max_fy = current_fy + 1
            fy_lookahead_filter = (
                f"AND RENEWAL_FISCAL_YEAR <= {max_fy}"
            )

        rollup_atr_outer = """COALESCE(
                    MAX(CASE WHEN TARGET_CLOUD = 'Commerce Cloud'
                             THEN RENEWAL_ATR_CONV END),
                    MAX(CASE WHEN TARGET_CLOUD LIKE '%Commerce%'
                             THEN RENEWAL_ATR_CONV END)
                )"""
        rollup_distinct_cols = """SELECT DISTINCT
                    COMBO_COMPANY_ID,
                    ACCT_ID,
                    ACCOUNT_NM,
                    ACCOUNT_18_ID,
                    RENEWAL_OPTY_ID,
                    RENEWAL_OPTY_ID_18,
                    RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV,
                    RENEWAL_AMT_CONV,
                    RENEWAL_ATR_CONV,
                    RENEWAL_FCAST_ATTRITION_CONV,
                    CONV_SWING_AMT,
                    RENEWAL_CLOSE_MONTH,
                    RENEWAL_CONTRACT_END_DATE,
                    RENEWAL_FISCAL_YEAR,
                    RENEWAL_STG_NM,
                    CSG_GEO,
                    CSG_TERRITORY,
                    TARGET_CLOUD"""
        dedup_where = f"""WHERE RENEWAL_CLOSE_MONTH >= '{min_esc}'
                  AND RENEWAL_STG_NM NOT IN ({_DEAD_STAGE_SQL})
                  {cloud_filter}
                  {opp_filter}
                  {fy_filter}
                  {fy_lookahead_filter}"""
        apm_anchor_where = f"""WHERE RENEWAL_CLOSE_MONTH >= '{min_esc}'
                  AND TARGET_CLOUD LIKE '%Commerce%'"""
        cloud_filter_r2 = _alias_predicate_for_outer_renewals_join(
            cloud_filter,
            "r2",
        )
        r2_anchor = (
            f" AND r2.RENEWAL_CLOSE_MONTH >= '{min_esc}'{cloud_filter_r2}"
        )

        rollup_inner_sql = f"""
        FROM (
            SELECT
                COMBO_COMPANY_ID,
                MAX(ACCOUNT_NM)                                     AS account_nm,
                MAX(ACCOUNT_18_ID)                                  AS account_18_id,
                MAX(ACCT_ID)                                        AS acct_id,
                MAX(RENEWAL_OPTY_ID_18)                             AS renewal_opty_id_18,
                SUM(RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV)       AS cc_aov_fallback,
                SUM(RENEWAL_AMT_CONV)                               AS forecasted,
                {rollup_atr_outer}                                   AS atr,
                SUM(RENEWAL_FCAST_ATTRITION_CONV)                   AS fcast_attrition,
                SUM(COALESCE(CONV_SWING_AMT, 0))                    AS conv_swing_amt,
                MIN(RENEWAL_CLOSE_MONTH)                            AS close_month,
                MAX(RENEWAL_CONTRACT_END_DATE)                     AS renewal_contract_end_date,
                MAX(RENEWAL_FISCAL_YEAR)                            AS fiscal_year,
                MAX(RENEWAL_STG_NM)                                 AS renewal_stage,
                MAX(CSG_GEO)                                        AS csg_geo,
                MAX(CSG_TERRITORY)                                  AS csg_territory,
                LISTAGG(DISTINCT TARGET_CLOUD, '; ')
                    WITHIN GROUP (ORDER BY TARGET_CLOUD)            AS all_clouds,
                COUNT(DISTINCT RENEWAL_OPTY_ID)                     AS opp_count,
                COUNT(DISTINCT ACCT_ID)                             AS account_count
            FROM (
                {rollup_distinct_cols}
                FROM {_PRIMARY_VIEW}
                {dedup_where}
            ) deduped
            GROUP BY COMBO_COMPANY_ID
            {having_sql}
        ) rollup"""

    sql = f"""
        SELECT
            rollup.COMBO_COMPANY_ID,
            rollup.account_nm,
            rollup.account_18_id,
            rollup.acct_id,
            rollup.renewal_opty_id_18,
            COALESCE(apm.cc_aov_apm, rollup.cc_aov_fallback)        AS cc_aov,
            rollup.forecasted,
            rollup.atr,
            rollup.fcast_attrition,
            rollup.conv_swing_amt,
            rollup.close_month,
            rollup.renewal_contract_end_date,
            rollup.fiscal_year,
            rollup.renewal_stage,
            rollup.csg_geo,
            rollup.csg_territory,
            rollup.all_clouds,
            rollup.opp_count,
            rollup.account_count,
            MAX(util.utilization_rate)                              AS utilization_rate,
            LISTAGG(DISTINCT p.APM_L1, ', ')
                WITHIN GROUP (ORDER BY p.APM_L1)                    AS all_products
        {rollup_inner_sql}
        LEFT JOIN (
            SELECT
                r.COMBO_COMPANY_ID,
                SUM(p.ATTR_BEGIN_AOV)                               AS cc_aov_apm
            FROM (
                SELECT DISTINCT COMBO_COMPANY_ID, ACCT_ID
                FROM {_PRIMARY_VIEW}
                {apm_anchor_where}
            ) r
            JOIN {_APM_VIEW} p
                ON r.ACCT_ID = p.ACCOUNT_ID
                AND p.APM_L1 = 'Commerce'
                AND p.SNAPSHOT_DT = (
                    SELECT MAX(SNAPSHOT_DT)
                    FROM {_APM_VIEW}
                )
            GROUP BY r.COMBO_COMPANY_ID
        ) apm ON rollup.COMBO_COMPANY_ID = apm.COMBO_COMPANY_ID
        LEFT JOIN (
            SELECT
                COMBO_COMPANY_ID,
                TRIM(
                    CASE WHEN b2c_prov > 0
                         THEN 'B2C: ' || TO_CHAR(ROUND(b2c_used / b2c_prov * 100, 1)) || '%'
                         ELSE '' END
                    || CASE WHEN ppo_prov > 0
                            THEN CASE WHEN b2c_prov > 0 THEN ' | ' ELSE '' END
                                 || 'PPO: ' || TO_CHAR(ROUND(ppo_used / ppo_prov * 100, 1)) || '%'
                            ELSE '' END
                    || CASE WHEN b2b_prov > 0
                            THEN ' | B2B: ' || TO_CHAR(ROUND(b2b_used / b2b_prov * 100, 1)) || '%'
                            ELSE '' END
                    || CASE WHEN oms_prov > 0
                            THEN ' | OMS: ' || TO_CHAR(ROUND(oms_used / oms_prov * 100, 1)) || '%'
                            ELSE '' END
                    || CASE WHEN d2c_prov > 0
                            THEN ' | D2C: ' || TO_CHAR(ROUND(d2c_used / d2c_prov * 100, 1)) || '%'
                            ELSE '' END
                )                                                   AS utilization_rate
            FROM (
                SELECT
                    COMBO_COMPANY_ID,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'B2C Commerce' AND GRP = 'GMV'
                             THEN PROVISIONED ELSE 0 END)           AS b2c_prov,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'B2C Commerce' AND GRP = 'GMV'
                             THEN USED ELSE 0 END)                  AS b2c_used,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'B2C Commerce' AND GRP = 'PPO'
                             THEN PROVISIONED ELSE 0 END)           AS ppo_prov,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'B2C Commerce' AND GRP = 'PPO'
                             THEN USED ELSE 0 END)                  AS ppo_used,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'Salesforce Commerce'
                              AND GRP = 'B2B Commerce Entitlement'
                              AND TYPE LIKE '%Orders Placed%'
                             THEN PROVISIONED ELSE 0 END)           AS b2b_prov,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'Salesforce Commerce'
                              AND GRP = 'B2B Commerce Entitlement'
                              AND TYPE LIKE '%Orders Placed%'
                             THEN USED ELSE 0 END)                  AS b2b_used,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'Salesforce Commerce'
                              AND GRP = 'OM Orders'
                              AND TYPE LIKE '%Managed Orders%'
                             THEN PROVISIONED ELSE 0 END)           AS oms_prov,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'Salesforce Commerce'
                              AND GRP = 'OM Orders'
                              AND TYPE LIKE '%Managed Orders%'
                             THEN USED ELSE 0 END)                  AS oms_used,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'Salesforce Commerce'
                              AND GRP = 'D2C Commerce Entitlement'
                              AND TYPE LIKE '%GMV Total%'
                             THEN PROVISIONED ELSE 0 END)           AS d2c_prov,
                    SUM(CASE WHEN DRVD_APM_LVL_2 = 'Salesforce Commerce'
                              AND GRP = 'D2C Commerce Entitlement'
                              AND TYPE LIKE '%GMV Total%'
                             THEN USED ELSE 0 END)                  AS d2c_used
                FROM {_USAGE_VIEW}
                WHERE CURR_SNAP_FLG = 'Y'
                  AND DRVD_APM_LVL_1 = 'Commerce'
                GROUP BY COMBO_COMPANY_ID
            ) util_agg
        ) util ON rollup.COMBO_COMPANY_ID = util.COMBO_COMPANY_ID
        JOIN {_PRIMARY_VIEW} r2
            ON r2.COMBO_COMPANY_ID = rollup.COMBO_COMPANY_ID
            {r2_anchor}
        LEFT JOIN {_APM_VIEW} p
            ON r2.ACCT_ID = p.ACCOUNT_ID
            AND p.SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM {_APM_VIEW}
            )
            AND p.APM_L1 IS NOT NULL
            AND p.APM_L1 != 'Core Success Plans'
        GROUP BY
            rollup.COMBO_COMPANY_ID, rollup.account_nm,
            rollup.account_18_id, rollup.acct_id,
            rollup.renewal_opty_id_18,
            COALESCE(apm.cc_aov_apm, rollup.cc_aov_fallback),
            rollup.forecasted, rollup.atr, rollup.fcast_attrition,
            rollup.conv_swing_amt,
            rollup.close_month, rollup.renewal_contract_end_date, rollup.fiscal_year,
            rollup.renewal_stage, rollup.csg_geo,
            rollup.csg_territory, rollup.all_clouds,
            rollup.opp_count, rollup.account_count
        ORDER BY cc_aov DESC
        LIMIT {int(limit)}
    """
    try:
        rows = run_query(
            sql,
            [],
            statement_timeout=_env_int("SNOWFLAKE_RENEWAL_STATEMENT_TIMEOUT", 120),
        )
    except Exception as e:
        log_debug(f"get_atrisk_renewals_bulk: {str(e)[:200]}")
        return []

    if not rows:
        return []
    return [_map_combo_rollup_to_bulk_schema(r) for r in rows]


def _build_cloud_filter(cloud: str) -> str:
    """Build cloud filter for RENEWALS view."""
    if not cloud:
        return ""
    c = str(cloud).strip().lower()
    if c == "all":
        return ""
    if "financial services" in c or c == "fsc":
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
