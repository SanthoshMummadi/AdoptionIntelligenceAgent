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

_snowflake_conn: Any = None


def get_snowflake_connection():
    """Singleton Snowflake connection (password or externalbrowser)."""
    global _snowflake_conn

    if _snowflake_conn is not None:
        return _snowflake_conn

    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH"
    database = os.getenv("SNOWFLAKE_DATABASE") or "SSE_DM_CSG_RPT_PRD"
    schema = os.getenv("SNOWFLAKE_SCHEMA") or "RENEWALS"
    role = os.getenv("SNOWFLAKE_ROLE")
    password = os.getenv("SNOWFLAKE_PASSWORD")

    if not account or not user:
        raise Exception("Missing SNOWFLAKE_ACCOUNT or SNOWFLAKE_USER in .env")

    params: dict[str, Any] = {
        "user": user,
        "account": account,
        "warehouse": warehouse,
        "database": database,
    }
    if schema:
        params["schema"] = schema
    if role:
        params["role"] = role

    if password:
        params["password"] = password
    else:
        params["authenticator"] = os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")

    _snowflake_conn = snowflake.connector.connect(**params)
    log_debug("✅ Connected to Snowflake")
    return _snowflake_conn


def run_query(sql: str, params: Optional[list] = None) -> list[dict]:
    """Execute Snowflake query; return list of row dicts (column names from cursor)."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params or [])
        rows = cursor.fetchall()
        if not cursor.description:
            return []
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        log_debug(f"Snowflake query error: {str(e)[:100]}")
        raise
    finally:
        cursor.close()


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
    try:
        num = float(val)
        if num >= 1_000_000:
            return f"${num / 1_000_000:.1f}M"
        if num >= 1000:
            return f"${num / 1000:.0f}K"
        return f"${num:.0f}"
    except (TypeError, ValueError):
        return str(val)


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
    Full enrichment: renewal AOV (drives usage), ARI, health.
    Usage comes from the renewal view only (FC_USAGE_BY_APM not used).
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
        "usage": {"utilization_rate": "N/A", "cloud_aov": "Unknown"},
    }

    if opty_id:
        try:
            renewal_data = get_renewal_aov(opty_id)
            if renewal_data:
                result["renewal_aov"] = renewal_data
                result["usage"] = {
                    "utilization_rate": "N/A",
                    "cloud_aov": fmt_amount(renewal_data.get("renewal_aov", 0)),
                    "clouds": [renewal_data.get("target_cloud") or ""],
                }
        except Exception as e:
            log_debug(f"Renewal AOV error: {str(e)[:60]}")

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
            account_ari = get_ari_score_by_account(account_id_15, cloud)
            if account_ari:
                first = account_ari[0]
                result["ari"] = {
                    "probability": first.get("ATTRITION_PROBA"),
                    "category": first.get("ATTRITION_PROBA_CATEGORY", "Unknown"),
                    "reason": first.get("ATTRITION_REASON", "N/A"),
                }
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


def get_renewal_aov(opty_id):
    """Get renewal AOV and ATR from WV_CI_RENEWAL_OPTY_VW - CORRECTED: Use 15-char ID"""
    if not opty_id:
        return {}
    opty_id_15 = to_15_char_id(opty_id)
    sql = """
        SELECT ACCOUNT_NM, TARGET_CLOUD,
               RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV,
               RENEWAL_FCAST_ATTRITION_CONV
        FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW
        WHERE RENEWAL_OPTY_ID = %s
        LIMIT 1
    """
    rows = run_query(sql, [opty_id_15])
    if rows:
        r = rows[0]
        return {
            "account_name": r.get("ACCOUNT_NM"),
            "target_cloud": r.get("TARGET_CLOUD"),
            "renewal_aov": float(r.get("RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV") or 0),
            "renewal_atr": abs(float(r.get("RENEWAL_FCAST_ATTRITION_CONV") or 0)),
        }
    return {}


def _apm_product_display_name(row: dict) -> str:
    """Prefer APM_LVL_3, then L2, then L1 (L3 is often NULL in CSS)."""
    for key in ("APM_LVL_3", "APM_LVL_2", "APM_LVL_1"):
        v = row.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return "Unknown"


def get_account_attrition(account_id: str, cloud: str | None = "Commerce Cloud") -> list:
    base = [
        "ACCOUNT_ID = %s",
        "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM "
        "SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT)",
    ]
    params: list[Any] = [to_15_char_id(account_id)]

    def _run(with_cloud: str | None) -> list:
        cond = list(base)
        if with_cloud:
            pred = apm_cloud_levels_predicate(with_cloud)
            if pred:
                cond.append(pred)
        where_clause = " AND ".join(cond)
        sql = f"""
            SELECT
                APM_LVL_1, APM_LVL_2, APM_LVL_3,
                ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
                ATTRITION_REASON, ATTRITION_PIPELINE
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE {where_clause}
            ORDER BY ATTRITION_PIPELINE DESC NULLS LAST
            LIMIT 20
        """
        return run_query(sql, params)

    use_cloud = cloud and str(cloud).strip() and str(cloud) != "All Clouds"
    rows = _run(cloud if use_cloud else None)
    if not rows and use_cloud:
        log_debug("get_account_attrition: no rows with cloud filter; retrying without")
        rows = _run(None)

    out = []
    for r in rows:
        out.append({
            "product": _apm_product_display_name(r),
            "APM_LVL_1": r.get("APM_LVL_1"),
            "APM_LVL_2": r.get("APM_LVL_2"),
            "APM_LVL_3": r.get("APM_LVL_3"),
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
    ari_prob = ari.get("probability", "")
    if ari_cat == "High":
        ari_emoji = ":red_circle:"
    elif ari_cat == "Medium":
        ari_emoji = ":large_yellow_circle:"
    elif ari_cat == "Low":
        ari_emoji = ":large_green_circle:"
    else:
        ari_emoji = ":white_circle:"

    prob_display = "N/A"
    if ari_prob is not None and ari_prob != "":
        try:
            prob_display = f"{float(ari_prob):.1f}"
        except (TypeError, ValueError):
            prob_display = str(ari_prob)

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
    return "\n".join([
        f"ARI: {display.get('ari_category', 'N/A')} ({display.get('ari_probability', 'N/A')}%)",
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
            "apm_lvl_3": r.get("APM_LVL_3"),
            "attrition_pipeline": float(r.get("ATTRITION_PIPELINE") or 0),
            "attrition_proba_category": r.get("ATTRITION_PROBA_CATEGORY"),
            "attrition_reason": r.get("ATTRITION_REASON"),
            "snapshot_dt": str(r.get("SNAPSHOT_DT") or ""),
        })
    return out


def _escape_sf_id(account_id: str) -> str:
    return str(account_id).replace("'", "")


class SnowflakeClient:
    """OOP wrapper (parallel GM workflow / adapters)."""

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
    ):
        self._conn: Any = None
        self._account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self._user = user or os.getenv("SNOWFLAKE_USER")
        self._password = password if password is not None else os.getenv("SNOWFLAKE_PASSWORD")
        self._warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH"
        self._database = database if database is not None else os.getenv("SNOWFLAKE_DATABASE")
        self._schema = schema if schema is not None else os.getenv("SNOWFLAKE_SCHEMA")
        self._role = role if role is not None else os.getenv("SNOWFLAKE_ROLE")
        self._authenticator = (
            authenticator if authenticator is not None else os.getenv("SNOWFLAKE_AUTHENTICATOR")
        )

        if not self._account or not self._user:
            raise ValueError("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER are required")

        conn_params: dict[str, Any] = {
            "account": self._account,
            "user": self._user,
            "warehouse": self._warehouse,
        }
        if self._database:
            conn_params["database"] = self._database
        if self._schema:
            conn_params["schema"] = self._schema
        if self._role:
            conn_params["role"] = self._role
        if self._password:
            conn_params["password"] = self._password
        else:
            conn_params["authenticator"] = self._authenticator or "externalbrowser"

        self._conn = snowflake.connector.connect(**conn_params)
        log_debug("✅ SnowflakeClient connected")

    def _cursor(self):
        return self._conn.cursor()

    def get_account_usage(self, account_id: str) -> Optional[dict[str, Any]]:
        aid = _escape_sf_id(account_id)
        cursor = self._cursor()
        try:
            query = f"""
            SELECT
                UTILIZATION_RATE,
                GMV_RATE,
                BURN_RATE,
                CC_AOV,
                TERRITORY,
                CSG_GEO
            FROM CIDM.WV_AV_USAGE_EXTRACT_VW
            WHERE ACCOUNT_ID = '{aid}'
            AND SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM CIDM.WV_AV_USAGE_EXTRACT_VW
            )
            LIMIT 1
            """
            cursor.execute(query)
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "utilization_rate": float(row[0]) if row[0] is not None else None,
                "gmv_rate": float(row[1]) if row[1] is not None else None,
                "burn_rate": float(row[2]) if row[2] is not None else None,
                "cc_aov": float(row[3]) if row[3] is not None else None,
                "territory": row[4],
                "csg_geo": row[5],
            }
        except Exception as e:
            log_error(f"SnowflakeClient.get_account_usage error: {e}")
            return None
        finally:
            cursor.close()

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
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as e:
                log_error(f"SnowflakeClient.close error: {e}")
            finally:
                self._conn = None
