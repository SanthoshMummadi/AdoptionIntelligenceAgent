"""
domain/analytics/snowflake_client.py
Snowflake enrichment — CSS attrition uses MAX(SNAPSHOT_DT) (no CURR_SNAP) + renewal view + shims.
"""
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Optional

import snowflake.connector
from dotenv import load_dotenv
from log_utils import log_debug, log_error

load_dotenv()

# Corporate suffix patterns for account-name stripping (fuzzy resolution)
CORPORATE_SUFFIXES = (
    r"\b(Inc\.?|LLC\.?|Ltd\.?|Corp\.?|Holdings?\.?|Holding\.?|Group|GmbH|Co\.?|"
    r"AB|SE|PLC|SA|AG|NV|BV|Pty\.?|Pte\.?|S\.A\.?|S\.L\.?|KGaA?|Limited|Company|"
    r"Enterprises?)\b"
)

SUCCESS_PLAN_KEYWORDS = [
    "success plan",
    "success plans",
    "- premier",
    "- signature",
    "- standard",
]

_sf_connection: Any = None


def _product_atr_amount(p: dict) -> float:
    """ABS attrition pipeline from raw Snowflake row or normalized get_account_attrition dict."""
    try:
        if p.get("ATTRITION_PIPELINE") is not None:
            return abs(float(p.get("ATTRITION_PIPELINE") or 0))
        return abs(float(p.get("attrition") or 0))
    except (TypeError, ValueError):
        return 0.0


def _product_proba(p: dict) -> float:
    try:
        v = p.get("ATTRITION_PROBA")
        if v is None:
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def is_success_plan(product: dict) -> bool:
    """True if APM L2/L3 looks like a Success Plan offer (excluded from overall ARI)."""
    l2 = str(product.get("APM_LVL_2") or "").lower()
    l3 = str(product.get("APM_LVL_3") or "").lower()
    return any(kw in l2 or kw in l3 for kw in SUCCESS_PLAN_KEYWORDS)


def calculate_overall_ari(products: list, min_atr_threshold: float = 0) -> dict:
    """
    Account-level ARI: exclude Success Plans, optional ATR floor, then sort by ATR then probability.
    """
    core = [p for p in products if not is_success_plan(p)]

    if not core:
        return {
            "category": "Unknown",
            "probability": None,
            "reason": "No qualifying products (all Success Plans)",
            "top_product": None,
            "atr_amount": 0,
        }

    qualified = [p for p in core if _product_atr_amount(p) >= min_atr_threshold]
    if not qualified:
        qualified = list(core)

    qualified.sort(
        key=lambda x: (_product_atr_amount(x), _product_proba(x)),
        reverse=True,
    )
    top = qualified[0]
    product_name = (
        top.get("APM_LVL_3")
        or top.get("APM_LVL_2")
        or top.get("APM_LVL_1")
        or top.get("product")
        or "Unknown"
    )

    return {
        "category": top.get("ATTRITION_PROBA_CATEGORY")
        or top.get("category", "Unknown"),
        "probability": top.get("ATTRITION_PROBA"),
        "reason": top.get("ATTRITION_REASON") or top.get("reason") or "N/A",
        "top_product": product_name,
        "atr_amount": _product_atr_amount(top),
    }


def split_products_by_type(products: list) -> dict:
    """Split into core vs success-plan rows; sort each by ATR desc then probability desc."""
    core: list = []
    success_plans: list = []
    for p in products:
        if is_success_plan(p):
            success_plans.append(p)
        else:
            core.append(p)
    sort_key = lambda x: (_product_atr_amount(x), _product_proba(x))
    core.sort(key=sort_key, reverse=True)
    success_plans.sort(key=sort_key, reverse=True)
    return {"core": core, "success_plans": success_plans}


def get_snowflake_connection():
    """
    Singleton Snowflake connection (password or externalbrowser).
    Reconnects if the session is closed or unhealthy.
    """
    global _sf_connection

    if _sf_connection is not None:
        try:
            if not _sf_connection.is_closed():
                return _sf_connection
        except Exception:
            pass
        log_debug("Snowflake connection lost — reconnecting...")
        try:
            _sf_connection.close()
        except Exception:
            pass
        _sf_connection = None

    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH"
    database = os.getenv("SNOWFLAKE_DATABASE") or "SSE_DM_CSG_RPT_PRD"
    schema = os.getenv("SNOWFLAKE_SCHEMA") or "RENEWALS"
    role = os.getenv("SNOWFLAKE_ROLE")
    password = os.getenv("SNOWFLAKE_PASSWORD")

    if not account or not user:
        raise Exception("Missing SNOWFLAKE_ACCOUNT or SNOWFLAKE_USER in .env")

    conn_params: dict[str, Any] = {
        "user": user,
        "account": account,
        "warehouse": warehouse,
        "database": database,
        "client_session_keep_alive": True,
    }
    if schema:
        conn_params["schema"] = schema
    if role:
        conn_params["role"] = role

    if password:
        conn_params["password"] = password
    else:
        conn_params["authenticator"] = os.getenv(
            "SNOWFLAKE_AUTHENTICATOR", "externalbrowser"
        )

    _sf_connection = snowflake.connector.connect(**conn_params)
    log_debug("✓ Connected to Snowflake")
    return _sf_connection


def run_query(sql: str, params: Optional[list] = None) -> list[dict]:
    """Execute Snowflake query; return list of row dicts (singleton connection)."""

    def _execute(conn: Any) -> list[dict]:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params or [])
            rows = cursor.fetchall()
            if not cursor.description:
                return []
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

    global _sf_connection
    conn = get_snowflake_connection()
    try:
        return _execute(conn)
    except Exception as e:
        error_str = str(e)
        log_debug(f"Snowflake query error: {error_str[:100]}")
        if any(
            kw in error_str.lower()
            for kw in (
                "connection",
                "session",
                "expired",
                "closed",
                "reset",
                "390114",
                "250002",
            )
        ):
            _sf_connection = None
            log_debug("Retrying Snowflake query with fresh connection...")
            try:
                return _execute(get_snowflake_connection())
            except Exception as retry_e:
                log_debug(f"Snowflake retry failed: {str(retry_e)[:100]}")
                raise
        raise


def to_15_char_id(account_id: str) -> str:
    if not account_id:
        return ""
    s = str(account_id)
    return s[:15] if len(s) > 15 else s


def apm_cloud_levels_predicate(cloud: str) -> str:
    """
    SQL ( ... ) over APM_LVL_1/2/3. Uses the full cloud label plus its first token
    (e.g. 'Commerce Cloud' -> also 'Commerce') so CSS rows match when levels omit 'Cloud'.
    """
    if not cloud or str(cloud).strip() == "" or str(cloud) == "All Clouds":
        return ""
    c = str(cloud).strip().replace("'", "''").replace("%", "%%")
    variants: list[str] = [c]
    first = c.split(None, 1)[0] if c else ""
    if first and first != c and len(first) >= 3:
        variants.append(first)
    seen: set[str] = set()
    uniq: list[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            uniq.append(v)
    parts: list[str] = []
    for v in uniq:
        for col in ("APM_LVL_1", "APM_LVL_2", "APM_LVL_3"):
            parts.append(f"{col} LIKE '%%{v}%%'")
    return f"({' OR '.join(parts)})"


def get_cloud_filter(cloud: str) -> str:
    """APM cloud match for SQL embedded alongside %s placeholders — %% survives pyformat."""
    return apm_cloud_levels_predicate(cloud)


def fmt_amount(val) -> str:
    """
    Format dollar amount — M shorthand (Option A).
    $695,492 → $0.7M ; $1,608,311 → $1.6M ; $0 → $0
    """
    try:
        num = float(val)
        if num == 0:
            return "$0"
        elif abs(num) >= 1_000:
            return f"${num / 1_000_000:.1f}M"
        else:
            return f"${num:.0f}"
    except (TypeError, ValueError):
        return str(val) if val else "N/A"


def get_usage_summary(account_id: str, cloud: str | None = None) -> dict:
    """
    Get usage from CIDM.WV_AV_USAGE_EXTRACT_VW.
    Priority: GMV row → Commerce L1/L2 → all products.
    """
    CLOUD_L1_MAP = {
        "Commerce Cloud": "Commerce",
        "B2C Commerce": "Commerce",
        "B2B Commerce": "Commerce",
        "Marketing Cloud": "Marketing",
        "Sales Cloud": "Sales",
        "Service Cloud": "Service",
        "Data Cloud": "AI and Data",
        "Tableau": "Analytics",
        "MuleSoft": "Integration",
    }

    def _build_cloud_filter(cloud_val: str | None) -> str:
        if not cloud_val or cloud_val == "All Clouds":
            return ""

        l1_value = CLOUD_L1_MAP.get(str(cloud_val).strip(), cloud_val)
        l1_safe = str(l1_value).replace("'", "''").replace("%", "%%")

        return f"""
            AND (
                DRVD_APM_LVL_1 LIKE '%%{l1_safe}%%'
                OR DRVD_APM_LVL_2 LIKE '%%{l1_safe}%%'
            )
        """

    cloud_filter = _build_cloud_filter(cloud)

    def _run_usage_query(snap_filter: str, params: list) -> list:
        sql = f"""
            SELECT
                DRVD_APM_LVL_1,
                DRVD_APM_LVL_2,
                GRP,
                TYPE,
                SUM(PROVISIONED) as TOTAL_PROV,
                SUM(ACTIVATED) as TOTAL_ACTIVATED,
                SUM(USED) as TOTAL_USED
            FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
            WHERE ACCOUNT_ID = %s
            {snap_filter}
            {cloud_filter}
            AND PROVISIONED > 0
            GROUP BY DRVD_APM_LVL_1, DRVD_APM_LVL_2, GRP, TYPE
            ORDER BY TOTAL_PROV DESC
        """
        try:
            return run_query(sql, params)
        except Exception as e:
            log_debug(f"get_usage_summary query error: {str(e)[:100]}")
            return []

    rows = _run_usage_query("AND CURR_SNAP_FLG = 'Y'", [account_id])

    if not rows:
        log_debug(
            "get_usage_summary: CURR_SNAP_FLG=Y returned nothing, trying MAX(SNAPSHOT_DT)"
        )
        rows = _run_usage_query(
            """
            AND SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
                WHERE ACCOUNT_ID = %s
            )
        """,
            [account_id, account_id],
        )

    if not rows:
        log_debug("get_usage_summary: trying without cloud filter, GMV only")
        try:
            rows = run_query(
                """
                SELECT
                    DRVD_APM_LVL_1, DRVD_APM_LVL_2,
                    GRP, TYPE,
                    SUM(PROVISIONED) as TOTAL_PROV,
                    SUM(ACTIVATED) as TOTAL_ACTIVATED,
                    SUM(USED) as TOTAL_USED
                FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
                WHERE ACCOUNT_ID = %s
                AND CURR_SNAP_FLG = 'Y'
                AND GRP = 'GMV'
                AND PROVISIONED > 0
                GROUP BY DRVD_APM_LVL_1, DRVD_APM_LVL_2, GRP, TYPE
            """,
                [account_id],
            )
        except Exception as e:
            log_debug(f"GMV fallback error: {str(e)[:100]}")

    if not rows:
        return {}

    gmv_rows = [r for r in rows if str(r.get("GRP", "")).upper() == "GMV"]

    if gmv_rows:
        total_prov = sum(float(r.get("TOTAL_PROV") or 0) for r in gmv_rows)
        total_used = sum(float(r.get("TOTAL_USED") or 0) for r in gmv_rows)
        source = "GMV"
    else:
        commerce_rows = [
            r
            for r in rows
            if "commerce" in str(r.get("DRVD_APM_LVL_1", "")).lower()
            or "commerce" in str(r.get("DRVD_APM_LVL_2", "")).lower()
        ]
        target_rows = commerce_rows if commerce_rows else rows
        total_prov = sum(float(r.get("TOTAL_PROV") or 0) for r in target_rows)
        total_used = sum(float(r.get("TOTAL_USED") or 0) for r in target_rows)
        source = "Commerce aggregate" if commerce_rows else "All products"

    if total_prov > 0:
        util_rate = (total_used / total_prov) * 100
        util_str = f"{util_rate:.1f}%"
    else:
        util_rate = 0
        util_str = "N/A"

    log_debug(
        f"✓ Usage ({source}): {util_str} util, prov={total_prov:,.0f}, used={total_used:,.0f}"
    )

    if util_rate >= 70:
        util_emoji = ":large_green_circle:"
    elif util_rate >= 40:
        util_emoji = ":large_yellow_circle:"
    elif util_rate > 0:
        util_emoji = ":red_circle:"
    else:
        util_emoji = ":white_circle:"

    return {
        "utilization_rate": util_str,
        "util_emoji": util_emoji,
        "cloud_aov": "Unknown",
        "gmv_util": util_str if gmv_rows else None,
        "source": source,
    }


def get_usage_raw_data(account_id: str, cloud: str | None = None) -> list:
    """
    Raw usage rows from CIDM.WV_AV_USAGE_EXTRACT_VW for build_adoption_pov().
    """
    def _run(snap_filter: str, params: list) -> list:
        sql = f"""
            SELECT
                DRVD_APM_LVL_1,
                DRVD_APM_LVL_2,
                GRP,
                TYPE,
                PROVISIONED,
                ACTIVATED,
                USED
            FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
            WHERE ACCOUNT_ID = %s
            {snap_filter}
            AND PROVISIONED > 0
            ORDER BY PROVISIONED DESC
        """
        try:
            return run_query(sql, params)
        except Exception as e:
            log_debug(f"get_usage_raw_data error: {str(e)[:100]}")
            return []

    rows = _run("AND CURR_SNAP_FLG = 'Y'", [account_id])

    if not rows:
        rows = _run(
            """
            AND SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
                WHERE ACCOUNT_ID = %s
            )
            """,
            [account_id, account_id],
        )

    return rows


def extract_usd(value) -> float:
    """Extract numeric USD value from various formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0


def resolve_money(snowflake_display: dict, opp: dict, field: str) -> str:
    """
    Resolve money fields with Snowflake-first, org62 fallback.
    field: one of "atr", "attrition", "aov", "swing".
    """
    if field == "atr":
        val = (snowflake_display or {}).get("renewal_aov", {}).get("renewal_atr")
        if not val:
            val = (snowflake_display or {}).get("renewal_atr")
        if not val and opp:
            val = extract_usd(opp.get("Forecasted_Attrition__c"))
        return fmt_amount(val) if val else "N/A"

    if field == "attrition":
        val = extract_usd(opp.get("Forecasted_Attrition__c")) if opp else None
        return fmt_amount(val) if val else "N/A"

    if field == "aov":
        val = (snowflake_display or {}).get("renewal_aov", {}).get("renewal_aov")
        if not val:
            val = (snowflake_display or {}).get("cc_aov")
            if isinstance(val, str):
                val = extract_usd(val)
        if not val and opp:
            val = extract_usd(opp.get("Amount"))
        return fmt_amount(val) if val else "N/A"

    if field == "swing":
        val = extract_usd(opp.get("Swing__c")) if opp else None
        return fmt_amount(val) if val else "N/A"

    return "N/A"


def enrich_account(account_id, opty_id=None, cloud=None):
    """
    Full enrichment: renewal AOV, CIDM usage (WV_AV_USAGE_EXTRACT_VW), ARI, health.
    """
    start = time.time()
    account_id_15 = to_15_char_id(account_id)

    result = {
        "ari": {
            "probability": None,
            "category": "Unknown",
            "reason": "N/A",
        },
        "renewal_aov": {},
        "health": {"overall_score": 0, "overall_literal": "Unknown"},
        "usage": {
            "utilization_rate": "N/A",
            "util_emoji": ":white_circle:",
            "cloud_aov": "Unknown",
            "gmv_util": None,
            "source": "",
        },
    }

    if opty_id:
        try:
            renewal_data = get_renewal_aov(opty_id)
            if renewal_data:
                result["renewal_aov"] = renewal_data
        except Exception as e:
            log_debug(f"Renewal AOV error: {str(e)[:60]}")

    # 4. Usage summary (CIDM.WV_AV_USAGE_EXTRACT_VW)
    try:
        usage_data = get_usage_summary(account_id_15, cloud)
        if usage_data:
            result["usage"] = {
                "utilization_rate": usage_data.get("utilization_rate", "N/A"),
                "util_emoji": usage_data.get("util_emoji", ":white_circle:"),
                "cloud_aov": usage_data.get("cloud_aov", "Unknown"),
                "gmv_util": usage_data.get("gmv_util"),
                "source": usage_data.get("source", ""),
            }
            if (
                result["usage"].get("cloud_aov") == "Unknown"
                and result.get("renewal_aov")
            ):
                result["usage"]["cloud_aov"] = fmt_amount(
                    result["renewal_aov"].get("renewal_aov", 0)
                )
            log_debug(
                f"✓ Usage: {usage_data.get('utilization_rate')} "
                f"({usage_data.get('source')})"
            )
        elif result.get("renewal_aov"):
            result["usage"]["cloud_aov"] = fmt_amount(
                result["renewal_aov"].get("renewal_aov", 0)
            )
    except Exception as e:
        log_debug(f"Usage fetch error: {str(e)[:60]}")

    try:
        if opty_id:
            ari_data = get_ari_score(opty_id)
            if ari_data:
                prob = ari_data.get("ATTRITION_PROBA")
                result["ari"] = {
                    "probability": prob,
                    "category": ari_data.get("ATTRITION_PROBA_CATEGORY", "Unknown"),
                    "reason": ari_data.get("ATTRITION_REASON", "N/A"),
                }

        if result["ari"]["category"] == "Unknown":
            all_products = get_account_attrition(account_id_15, cloud)
            if all_products:
                ari_result = calculate_overall_ari(
                    all_products, min_atr_threshold=0
                )
                result["ari"] = {
                    "probability": ari_result["probability"],
                    "category": ari_result["category"],
                    "reason": ari_result["reason"],
                    "top_product": ari_result.get("top_product"),
                    "atr_amount": ari_result.get("atr_amount", 0),
                }
                log_debug(
                    "ARI from account-level: "
                    f"{ari_result['category']} via {ari_result.get('top_product')}"
                )
    except Exception as e:
        log_debug(f"ARI fetch error: {str(e)[:60]}")

    try:
        health_data = get_customer_health(account_id_15)
        if health_data:
            result["health"] = health_data
    except Exception as e:
        log_debug(f"Health fetch error: {str(e)[:60]}")

    log_debug(f"✓ enrich_account took {time.time() - start:.2f}s")
    return result


def get_ari_score(opty_id):
    """Get ARI for specific opportunity - CORRECTED: Use 15-char ID and MAX(SNAPSHOT_DT)"""
    if not opty_id:
        return None
    opty_id_15 = to_15_char_id(opty_id)
    sql = """
        SELECT ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
               ATTRITION_REASON
        FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_OPPTY
        WHERE RENEWAL_OPTY_ID = %s
        AND SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_OPPTY)
        LIMIT 1
    """
    rows = run_query(sql, [opty_id_15])
    return rows[0] if rows else None


def get_ari_score_by_account(account_id: str, cloud: str | None = "Commerce Cloud") -> list:
    """ARI rows for account on latest CSS ATTRITION_PREDICTION_ACCT_PRODUCT snapshot."""
    base = [
        "ACCOUNT_ID = %s",
        "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM "
        "SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT)",
    ]
    params: list[Any] = [account_id]

    def _run(with_cloud: str | None) -> list:
        cond = list(base)
        if with_cloud:
            pred = apm_cloud_levels_predicate(with_cloud)
            if pred:
                cond.append(pred)
        sql = f"""
            SELECT ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
                   ATTRITION_REASON, APM_LVL_2, APM_LVL_3
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE {' AND '.join(cond)}
            ORDER BY ATTRITION_PROBA DESC NULLS LAST
            LIMIT 5
        """
        return run_query(sql, params)

    use_cloud = cloud and str(cloud).strip() and str(cloud) != "All Clouds"
    rows = _run(cloud if use_cloud else None)
    if not rows and use_cloud:
        log_debug("get_ari_score_by_account: no rows with cloud filter; retrying without")
        rows = _run(None)
    return rows


def get_customer_health(account_id):
    """Fetch customer health score - CORRECTED: Use MAX(SNAPSHOT_DT)"""
    sql = """
        SELECT CATEGORY, SUB_CATEGORY,
               OVERALL_SCORE, CATEGORY_SCORE,
               OVERALL_LITERAL_SCORE, CATEGORY_LITERAL_SCORE
        FROM SSE_DM_CSG_RPT_PRD.CSS.CI_CH_FACT_CUSTOMER_HEALTH_VW
        WHERE ACCOUNT_ID = %s
        AND SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM SSE_DM_CSG_RPT_PRD.CSS.CI_CH_FACT_CUSTOMER_HEALTH_VW)
        ORDER BY CATEGORY
        LIMIT 20
    """
    rows = run_query(sql, [account_id])

    if not rows:
        return None

    return {
        "overall_score": rows[0].get("OVERALL_SCORE"),
        "overall_literal": rows[0].get("OVERALL_LITERAL_SCORE", "Unknown"),
        "categories": [{
            "category": r.get("CATEGORY"),
            "score": r.get("CATEGORY_SCORE"),
            "literal": r.get("CATEGORY_LITERAL_SCORE"),
        } for r in rows],
    }


_GMV_RATE_COLUMN_PREFERENCE: tuple[str, ...] = (
    "RENEWAL_GMV_UTIL_PCT",
    "RENEWAL_GMV_UTILIZATION_PCT",
    "GMV_UTILIZATION_RATE",
    "RENEWAL_GMV_RATE_PCT",
    "GMV_RATE_PCT",
    "RENEWAL_GMV_RATE",
    "GMV_RATE",
    "RENEWAL_GMV_UTLZTN_PCT",
    "GMV_UTLZN_RATE",
)


def _format_gmv_rate_for_display(val: Any) -> Optional[str]:
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        s = str(val).strip()
        return s if s else None
    if 0 <= f <= 1.0:
        return f"{f * 100:.1f}%"
    return f"{f:.1f}%"


def _gmv_rate_pct_from_renewal_row(row: dict) -> Optional[str]:
    """Pick GMV rate from WV_CI_RENEWAL_OPTY_VW row (SELECT *); prefers known column names."""
    if not row:
        return None
    for key in _GMV_RATE_COLUMN_PREFERENCE:
        if key in row and row[key] is not None:
            out = _format_gmv_rate_for_display(row[key])
            if out:
                return out
    for k, v in row.items():
        if v is None:
            continue
        ku = str(k).upper()
        if "GMV" in ku and any(
            x in ku for x in ("RATE", "UTIL", "PCT", "UTLZ", "BURN")
        ):
            out = _format_gmv_rate_for_display(v)
            if out:
                return out
    return None


def get_renewal_aov(opty_id):
    """Renewal row from WV_CI_RENEWAL_OPTY_VW (AOV, ATR, CSG_GEO, GMV rate from view)."""
    if not opty_id:
        return {}
    opty_id_15 = to_15_char_id(opty_id)
    sql = """
        SELECT *
        FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW
        WHERE RENEWAL_OPTY_ID = %s
        LIMIT 1
    """
    rows = run_query(sql, [opty_id_15])
    if rows:
        r = rows[0]
        out: dict[str, Any] = {
            "account_name": r.get("ACCOUNT_NM"),
            "target_cloud": r.get("TARGET_CLOUD"),
            "renewal_aov": float(r.get("RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV") or 0),
            "renewal_atr": abs(float(r.get("RENEWAL_FCAST_ATTRITION_CONV") or 0)),
            "csg_geo": r.get("CSG_GEO") or "",
        }
        gmv = _gmv_rate_pct_from_renewal_row(r)
        if gmv:
            out["gmv_rate_pct"] = gmv
        return out
    return {}


def _apm_product_display_name(row: dict) -> str:
    """Prefer APM_LVL_3, then L2, then L1 (L3 is often NULL in CSS)."""
    for key in ("APM_LVL_3", "APM_LVL_2", "APM_LVL_1"):
        v = row.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return "Unknown"


def get_account_attrition(account_id: str, cloud: str | None = "Commerce Cloud") -> list:
    """
    Product-level attrition on latest CSS snapshot.
    ``cloud=None`` (or empty / ``All Clouds``): all products, no APM cloud predicate.
    """
    conditions = [
        "ACCOUNT_ID = %s",
        "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM "
        "SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT)",
    ]
    params: list[Any] = [to_15_char_id(account_id)]

    use_cloud = (
        cloud is not None
        and str(cloud).strip()
        and str(cloud) != "All Clouds"
    )

    def _run(extra_predicate: str | None) -> list:
        cond = list(conditions)
        if extra_predicate:
            cond.append(extra_predicate)
        where_clause = " AND ".join(cond)
        sql = f"""
            SELECT
                APM_LVL_1, APM_LVL_2, APM_LVL_3,
                ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
                ATTRITION_REASON, ATTRITION_PIPELINE
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE {where_clause}
            ORDER BY ATTRITION_PIPELINE DESC NULLS LAST
            LIMIT 50
        """
        return run_query(sql, params)

    pred = apm_cloud_levels_predicate(str(cloud).strip()) if use_cloud else ""
    rows = _run(pred if pred else None)
    if not rows and use_cloud and pred:
        log_debug("get_account_attrition: no rows with cloud filter; retrying without")
        rows = _run(None)

    out = []
    for r in rows:
        out.append({
            "product": _apm_product_display_name(r),
            "APM_LVL_1": r.get("APM_LVL_1"),
            "APM_LVL_2": r.get("APM_LVL_2"),
            "APM_LVL_3": r.get("APM_LVL_3"),
            "ATTRITION_PIPELINE": r.get("ATTRITION_PIPELINE"),
            "ATTRITION_PROBA": r.get("ATTRITION_PROBA"),
            "ATTRITION_PROBA_CATEGORY": r.get("ATTRITION_PROBA_CATEGORY"),
            "ATTRITION_REASON": r.get("ATTRITION_REASON"),
            "attrition": abs(float(r.get("ATTRITION_PIPELINE") or 0)),
            "category": r.get("ATTRITION_PROBA_CATEGORY"),
            "reason": r.get("ATTRITION_REASON") or "",
            "factors_incr": r.get("FACTORS_INCR_RISK") or "",
            "factors_decr": r.get("FACTORS_DECR_RISK") or "",
        })
    return out


def format_enrichment_for_display(enrichment: dict) -> dict:
    if not enrichment:
        return {}

    ari = enrichment.get("ari", {})
    ari_cat = ari.get("category", "Unknown")
    if ari_cat == "High":
        ari_emoji = ":red_circle:"
    elif ari_cat == "Medium":
        ari_emoji = ":large_yellow_circle:"
    elif ari_cat == "Low":
        ari_emoji = ":large_green_circle:"
    else:
        ari_emoji = ":white_circle:"

    ari_prob = ari.get("probability")
    if ari_prob is not None:
        try:
            prob_float = float(ari_prob)
            if prob_float <= 1.0:
                prob_display = f"{prob_float * 100:.1f}%"
            else:
                prob_display = f"{prob_float:.1f}%"
        except (TypeError, ValueError):
            prob_display = "N/A"
    else:
        prob_display = "N/A"

    result = {
        "ari_category": ari_cat,
        "ari_probability": prob_display,
        "ari_emoji": ari_emoji,
        "ari_reason": ari.get("reason", "N/A"),
        "territory": "N/A",
        "csg_geo": "N/A",
        "burn_rate": "N/A",
        "gmv_rate": "N/A",
    }

    health = enrichment.get("health", {})
    health_score = health.get("overall_score")
    health_literal = health.get("overall_literal", "Unknown")
    score_val = None
    if health_literal not in (None, "", "Unknown"):
        if isinstance(health_literal, (int, float)):
            try:
                score_val = float(health_literal)
            except (TypeError, ValueError):
                pass
        elif str(health_literal).isdigit():
            score_val = float(health_literal)
    if score_val is not None:
        if score_val >= 70:
            health_literal = "Green"
        elif score_val >= 40:
            health_literal = "Yellow"
        else:
            health_literal = "Red"
    if health_score:
        try:
            hs = float(health_score)
            if hs >= 70:
                health_display = f":large_green_circle: Green ({int(hs)})"
            elif hs >= 40:
                health_display = f":large_yellow_circle: Yellow ({int(hs)})"
            else:
                health_display = f":red_circle: Red ({int(hs)})"
        except (TypeError, ValueError):
            health_display = ":white_circle: Unknown"
    else:
        health_display = ":white_circle: Unknown"

    result["health_score"] = health_score
    result["health_literal"] = health_literal
    result["health_display"] = health_display

    usage = enrichment.get("usage", {})
    cloud_aov = usage.get("cloud_aov", "Unknown")
    util_rate = usage.get("utilization_rate", "Unknown")
    util_emoji = ":white_circle:"
    if util_rate not in ("Unknown", "N/A", None, ""):
        try:
            util_val = float(str(util_rate).rstrip("%").strip())
            if util_val >= 70:
                util_emoji = ":large_green_circle:"
            elif util_val >= 40:
                util_emoji = ":large_yellow_circle:"
            else:
                util_emoji = ":red_circle:"
        except (TypeError, ValueError):
            pass

    result["cc_aov"] = cloud_aov
    result["utilization_rate"] = util_rate
    result["util_emoji"] = util_emoji

    renewal = enrichment.get("renewal_aov") or {}
    if renewal:
        result["renewal_aov"] = renewal
        if renewal.get("renewal_atr") is not None:
            result["renewal_atr"] = renewal.get("renewal_atr")
        if renewal.get("gmv_rate_pct") is not None:
            result["gmv_rate"] = renewal.get("gmv_rate_pct")
    return result


def format_enrichment_for_claude(enrichment: dict) -> str:
    if not enrichment:
        return ""
    display = format_enrichment_for_display(enrichment)
    _ap = display.get("ari_probability", "N/A")
    _ap_paren = (
        _ap
        if (_ap == "N/A" or str(_ap).strip().endswith("%"))
        else f"{_ap}%"
    )
    return "\n".join([
        f"ARI: {display.get('ari_category', 'N/A')} ({_ap_paren})",
        f"Utilization: {display.get('utilization_rate', 'N/A')}",
        f"GMV Rate: {display.get('gmv_rate', 'N/A')}",
        f"Territory: {display.get('territory', 'N/A')}",
        f"Health: {display.get('health_display', 'N/A')}",
    ])


def _resolve_account_from_snowflake_css(account_name: str) -> Optional[dict]:
    """Last-resort name match on latest CSS attrition snapshot."""
    if not account_name:
        return None
    try:
        rows = run_query(
            """
            SELECT DISTINCT ACCOUNT_ID, ACCOUNT_NAME
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE UPPER(ACCOUNT_NAME) LIKE UPPER(%s)
            AND SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            )
            LIMIT 1
            """,
            [f"%{account_name.strip()}%"],
        )
        if rows:
            return {
                "account_id": rows[0].get("ACCOUNT_ID"),
                "account_name": rows[0].get("ACCOUNT_NAME"),
            }
    except Exception as e:
        log_debug(f"CSS Snowflake account resolve error: {str(e)[:80]}")
    return None


def resolve_account_from_snowflake(
    name: str, cloud: str = "Commerce Cloud"
) -> Optional[dict]:
    """
    Resolve account from Snowflake renewal view using parallel fuzzy LIKE patterns.
    Returns: {"account_id", "account_name"} or None.
    """
    if not name:
        return None

    search_clean = name.strip()
    search_stripped = re.sub(
        CORPORATE_SUFFIXES, "", search_clean, flags=re.IGNORECASE
    ).strip().rstrip(",").strip()
    search_words = search_stripped.split()

    patterns: list[str] = [search_clean, f"%{search_clean}%"]
    if search_stripped:
        patterns.append(f"%{search_stripped}%")
    if search_words and len(search_words[0]) > 3:
        patterns.append(f"%{search_words[0]}%")

    def try_pattern(pattern: str, priority: int) -> Optional[dict]:
        try:
            cloud_safe = str(cloud).replace("'", "''").replace("%", "%%")
            sql = f"""
                SELECT DISTINCT
                    ren.ACCOUNT_18_ID AS ACCOUNT_ID,
                    ren.ACCOUNT_NM AS ACCOUNT_NAME
                FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW ren
                WHERE ren.ACCOUNT_NM LIKE %s
                AND (ren.TARGET_CLOUD LIKE '%%{cloud_safe}%%'
                     OR ren.RENEWAL_OPTY_NM LIKE '%%{cloud_safe}%%')
                ORDER BY ren.RENEWAL_CLSD_DT DESC NULLS LAST
                LIMIT 1
            """
            rows = run_query(sql, [pattern])
            if rows:
                return {
                    "account_id": rows[0].get("ACCOUNT_ID"),
                    "account_name": rows[0].get("ACCOUNT_NAME"),
                    "priority": priority,
                }
        except Exception as e:
            log_debug(f"Snowflake pattern resolve error: {str(e)[:60]}")
        return None

    best: Optional[dict] = None
    best_priority = 999
    with ThreadPoolExecutor(max_workers=max(1, len(patterns))) as executor:
        future_map = {
            executor.submit(try_pattern, p, idx): idx
            for idx, p in enumerate(patterns)
        }
        for fut in as_completed(future_map, timeout=25):
            try:
                res = fut.result(timeout=10)
                if res and res["priority"] < best_priority:
                    best = res
                    best_priority = res["priority"]
                    if best_priority == 0:
                        break
            except Exception:
                continue

    if best:
        return {"account_id": best["account_id"], "account_name": best["account_name"]}

    return _resolve_account_from_snowflake_css(search_clean)


def get_at_risk_accounts_snowflake(
    cloud: str | None = None,
    risk_category: str | None = None,
    min_attrition: float = 0,
    limit: int = 25,
    min_aov: float = 0,
    ari_filter: str | None = None,
    sort_by: str = "atr",
):
    del min_aov
    conditions = [
        "ACCOUNT_ID IS NOT NULL",
        "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM "
        "SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT)",
    ]

    if cloud and cloud != "All Clouds":
        pred = apm_cloud_levels_predicate(cloud)
        if pred:
            conditions.append(pred)

    if ari_filter:
        safe = str(ari_filter).replace("'", "''")
        conditions.append(f"ATTRITION_PROBA_CATEGORY = '{safe}'")
    elif risk_category:
        safe_rc = str(risk_category).replace("'", "''")
        conditions.append(f"ATTRITION_PROBA_CATEGORY = '{safe_rc}'")

    if min_attrition > 0:
        conditions.append(f"ABS(ATTRITION_PIPELINE) > {min_attrition}")

    conditions.append(
        "LOWER(COALESCE(APM_LVL_2, '')) NOT LIKE '%success plan%'"
    )
    conditions.append(
        "LOWER(COALESCE(APM_LVL_3, '')) NOT LIKE '%success plan%'"
    )

    where_clause = " AND ".join(conditions)
    sort_map = {
        "atr": "ABS(ATTRITION_PIPELINE) DESC",
        "ari": "ATTRITION_PROBA DESC",
        "aov": "ATTRITION_PIPELINE DESC",
        "cc_aov": "ATTRITION_PIPELINE DESC",
    }
    order_by = sort_map.get(sort_by, "ABS(ATTRITION_PIPELINE) DESC")

    sql = f"""
        SELECT
            ACCOUNT_ID,
            APM_LVL_1, APM_LVL_2, APM_LVL_3,
            ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
            ATTRITION_REASON, ATTRITION_PIPELINE,
            SNAPSHOT_DT
        FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE {where_clause}
        ORDER BY {order_by}
        LIMIT {int(limit)}
    """
    try:
        raw = run_query(sql, [])
    except Exception as e:
        log_error(f"get_at_risk_accounts_snowflake error: {e}")
        return []

    out = []
    for r in raw:
        out.append({
            "account_id": r.get("ACCOUNT_ID"),
            "account_name": r.get("ACCOUNT_NAME") or "",
            "apm_lvl_1": r.get("APM_LVL_1"),
            "apm_lvl_2": r.get("APM_LVL_2"),
            "apm_lvl_3": r.get("APM_LVL_3"),
            # Uppercase aliases for callers/tests expecting SQL-style keys
            "ACCOUNT_ID": r.get("ACCOUNT_ID"),
            "APM_LVL_2": r.get("APM_LVL_2"),
            "APM_LVL_3": r.get("APM_LVL_3"),
            "ATTRITION_PROBA_CATEGORY": r.get("ATTRITION_PROBA_CATEGORY"),
            "ATTRITION_PIPELINE": r.get("ATTRITION_PIPELINE"),
            "attrition_pipeline": float(r.get("ATTRITION_PIPELINE") or 0),
            "attrition_proba_category": r.get("ATTRITION_PROBA_CATEGORY"),
            "attrition_reason": r.get("ATTRITION_REASON"),
            "snapshot_dt": str(r.get("SNAPSHOT_DT") or ""),
        })
    return out


def _escape_sf_id(account_id: str) -> str:
    return str(account_id).replace("'", "")


class SnowflakeClient:
    """Singleton OOP wrapper; always uses module-level get_snowflake_connection()."""

    _instance: Optional["SnowflakeClient"] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "SnowflakeClient":
        # args/kwargs are for __init__ only; Python still passes them to __new__
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._initialized = False
            cls._instance = inst
        return cls._instance

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        authenticator: Optional[str] = None,
    ) -> None:
        if self._initialized:
            return
        self._account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self._user = user or os.getenv("SNOWFLAKE_USER")
        if not self._account or not self._user:
            raise ValueError("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER are required")
        get_snowflake_connection()
        self._initialized = True

    @property
    def conn(self) -> Any:
        """Always the current module singleton (stays valid after reconnect)."""
        return get_snowflake_connection()

    def _cursor(self):
        return self.conn.cursor()

    def get_account_usage(
        self, account_id: str, cloud: str = "Commerce Cloud"
    ) -> Optional[dict[str, Any]]:
        try:
            usage = get_usage_summary(to_15_char_id(account_id), cloud)
            if usage:
                return {
                    "utilization_rate": usage.get("utilization_rate", "N/A"),
                    "util_emoji": usage.get("util_emoji", ":white_circle:"),
                    "gmv_util": usage.get("gmv_util"),
                    "source": usage.get("source", ""),
                }
        except Exception as e:
            log_error(f"SnowflakeClient.get_account_usage error: {str(e)[:100]}")
        return None

    def get_ari_score(self, account_id: str) -> Optional[float]:
        aid = _escape_sf_id(account_id)
        cursor = self._cursor()
        try:
            query = f"""
            SELECT ATTRITION_PROBA * 100 AS probability
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE ACCOUNT_ID = '{aid}'
            AND SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            )
            ORDER BY ATTRITION_PROBA DESC
            LIMIT 1
            """
            cursor.execute(query)
            row = cursor.fetchone()
            if not row or row[0] is None:
                return None
            return round(float(row[0]), 1)
        except Exception as e:
            log_error(f"SnowflakeClient.get_ari_score error: {e}")
            return None
        finally:
            cursor.close()

    def get_attrition_signals(self, account_id: str) -> Optional[dict[str, Any]]:
        aid = _escape_sf_id(account_id)
        cursor = self._cursor()
        try:
            query = f"""
            SELECT
                APM_LVL_3 AS product,
                ABS(ATTRITION_PIPELINE) AS attrition,
                ATTRITION_PROBA_CATEGORY AS category
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE ACCOUNT_ID = '{aid}'
            AND SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            )
            ORDER BY ABS(ATTRITION_PIPELINE) DESC
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            products = [
                {
                    "product": r[0],
                    "attrition": float(r[1]) if r[1] is not None else 0.0,
                    "category": r[2],
                }
                for r in rows
            ]
            return {"account_id": account_id, "products": products, "count": len(products)}
        except Exception as e:
            log_error(f"SnowflakeClient.get_attrition_signals error: {e}")
            return None
        finally:
            cursor.close()

    def close(self) -> None:
        """No-op: singleton connection is shared; do not close from here."""
        pass


# --- SF Products label cleanup (canvas, Sheets, exporters) -----------------

APM_L1_DISPLAY_MAP: dict[str, str] = {
    "Salesforce Platform": "Platform",
    "Integration": "MuleSoft",
    "AI and Data": "Data Cloud",
    "Cross Cloud - CRM": "CRM",
    "Cross Cloud - Einstein": "Einstein",
}

APM_L1_EXCLUDE = frozenset({"Other", ""})


def get_sf_products_display(all_products: list) -> str:
    """Deduped APM L1 labels for Salesforce products (maps long L1 names to short labels)."""
    if not all_products:
        return "N/A"

    unique_l1s = list(
        dict.fromkeys(
            str(p.get("APM_LVL_1") or "").strip()
            for p in all_products
            if str(p.get("APM_LVL_1") or "").strip()
        )
    )

    cleaned: list[str] = []
    for l1 in unique_l1s:
        if l1 in APM_L1_EXCLUDE:
            continue
        display = APM_L1_DISPLAY_MAP.get(l1, l1)
        if display not in cleaned:
            cleaned.append(display)

    return ", ".join(cleaned) if cleaned else "N/A"
