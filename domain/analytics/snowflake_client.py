"""
snowflake_client.py
Snowflake analytics client for Commerce Cloud data.
"""
import os
import snowflake.connector
from log_utils import log_debug, log_error

_snowflake_conn = None


def get_snowflake_connection():
    """Get or create Snowflake connection."""
    global _snowflake_conn

    if _snowflake_conn is None:
        account = os.environ.get("SNOWFLAKE_ACCOUNT")
        user = os.environ.get("SNOWFLAKE_USER")
        password = os.environ.get("SNOWFLAKE_PASSWORD")
        warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

        if not all([account, user]):
            raise Exception("Missing Snowflake credentials in .env")

        connect_kwargs = {
            "account": account,
            "user": user,
            "warehouse": warehouse,
        }
        if password:
            connect_kwargs["password"] = password
        else:
            connect_kwargs["authenticator"] = "externalbrowser"

        _snowflake_conn = snowflake.connector.connect(**connect_kwargs)
        log_debug("✅ Connected to Snowflake")

    return _snowflake_conn


def resolve_account_from_snowflake(account_name: str, cloud: str = "Commerce Cloud") -> dict:
    """Resolve account name from Snowflake."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        query = f"""
        SELECT DISTINCT
            SF_ACCOUNT_ID,
            ACCOUNT_NAME
        FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE UPPER(ACCOUNT_NAME) LIKE '%{account_name.upper()}%'
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ATTRITION_PREDICTION_ACCT_PRODUCT
        )
        LIMIT 1
        """
        cursor.execute(query)
        row = cursor.fetchone()

        if row:
            return {
                "sf_account_id": row[0],
                "account_name": row[1],
            }
        return None

    except Exception as e:
        log_error(f"resolve_account_from_snowflake error: {e}")
        return None
    finally:
        cursor.close()


def get_ari_score_by_account(account_id: str, cloud: str = "Commerce Cloud") -> list:
    """Get ARI scores for account."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        query = f"""
        SELECT
            APM_LVL_3 as product,
            ATTRITION_PROBA_CATEGORY as category,
            ATTRITION_PROBABILITY * 100 as probability,
            ATTRITION_REASON as reason
        FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE SF_ACCOUNT_ID = '{account_id}'
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ATTRITION_PREDICTION_ACCT_PRODUCT
        )
        ORDER BY ATTRITION_PROBABILITY DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                "product": row[0],
                "category": row[1],
                "probability": round(float(row[2]), 1) if row[2] else 0,
                "reason": row[3],
            })
        return results

    except Exception as e:
        log_error(f"get_ari_score error: {e}")
        return []
    finally:
        cursor.close()


def get_at_risk_accounts_snowflake(
    cloud: str = "Commerce Cloud",
    risk_category: str = None,
    min_attrition: float = 0,
    limit: int = 25,
    min_aov: float = 0,
    ari_filter: str = None,
    sort_by: str = "atr",
) -> list:
    """Get at-risk accounts from Snowflake."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        where_clauses = [
            "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ATTRITION_PREDICTION_ACCT_PRODUCT)"
        ]

        if min_attrition > 0:
            where_clauses.append(f"ABS(ATTRITION_PIPELINE) >= {min_attrition}")

        if ari_filter:
            where_clauses.append(f"ATTRITION_PROBA_CATEGORY = '{ari_filter}'")

        where_sql = " AND ".join(where_clauses)

        sort_map = {
            "atr": "ABS(ATTRITION_PIPELINE) DESC",
            "ari": "ATTRITION_PROBABILITY DESC",
            "cc_aov": "CC_AOV DESC",
        }
        order_by = sort_map.get(sort_by, "ABS(ATTRITION_PIPELINE) DESC")

        query = f"""
        SELECT
            SF_ACCOUNT_ID as account_id,
            ACCOUNT_NAME as account_name,
            APM_LVL_3 as apm_lvl_3,
            ABS(ATTRITION_PIPELINE) as attrition_pipeline,
            ATTRITION_PROBA_CATEGORY as attrition_proba_category,
            ATTRITION_REASON as attrition_reason,
            SNAPSHOT_DT as snapshot_dt
        FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE {where_sql}
        ORDER BY {order_by}
        LIMIT {limit}
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                "account_id": row[0],
                "account_name": row[1],
                "apm_lvl_3": row[2],
                "attrition_pipeline": float(row[3]) if row[3] else 0,
                "attrition_proba_category": row[4],
                "attrition_reason": row[5],
                "snapshot_dt": str(row[6]) if row[6] else "",
            })
        return results

    except Exception as e:
        log_error(f"get_at_risk_accounts error: {e}")
        return []
    finally:
        cursor.close()


def enrich_account(account_id: str, opp_id: str, cloud: str) -> dict:
    """Enrich account with Snowflake data."""
    try:
        ari_scores = get_ari_score_by_account(account_id, cloud)

        # Get utilization and other metrics
        conn = get_snowflake_connection()
        cursor = conn.cursor()

        query = f"""
        SELECT
            UTILIZATION_RATE,
            GMV_RATE,
            BURN_RATE,
            CC_AOV,
            TERRITORY,
            CSG_GEO
        FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ACCOUNT_METRICS
        WHERE SF_ACCOUNT_ID = '{account_id}'
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ACCOUNT_METRICS
        )
        LIMIT 1
        """

        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()

        usage = {}
        if row:
            usage = {
                "utilization_rate": f"{float(row[0]):.1f}%" if row[0] else "N/A",
                "gmv_rate": f"{float(row[1]):.1f}%" if row[1] else "N/A",
                "burn_rate": f"{float(row[2]):.1f}%" if row[2] else "N/A",
                "cc_aov": f"${float(row[3]):,.0f}" if row[3] else "N/A",
                "territory": row[4] or "N/A",
                "csg_geo": row[5] or "N/A",
            }

        return {
            "ari_scores": ari_scores,
            "usage": usage,
            "degraded": [],
        }

    except Exception as e:
        log_error(f"enrich_account error: {e}")
        return {"ari_scores": [], "usage": {}, "degraded": ["snowflake"]}


def format_enrichment_for_display(enrichment: dict) -> dict:
    """Format enrichment data for display."""
    if not enrichment:
        return {}

    ari_scores = enrichment.get("ari_scores", [])
    usage = enrichment.get("usage", {})

    display = {
        "ari_category": ari_scores[0].get("category") if ari_scores else "N/A",
        "ari_probability": ari_scores[0].get("probability") if ari_scores else "N/A",
        "ari_reason": ari_scores[0].get("reason") if ari_scores else "N/A",
        "utilization_rate": usage.get("utilization_rate", "N/A"),
        "gmv_rate": usage.get("gmv_rate", "N/A"),
        "burn_rate": usage.get("burn_rate", "N/A"),
        "cc_aov": usage.get("cc_aov", "N/A"),
        "territory": usage.get("territory", "N/A"),
        "csg_geo": usage.get("csg_geo", "N/A"),
    }

    return display


def format_enrichment_for_claude(enrichment: dict) -> str:
    """Format enrichment for Claude context."""
    if not enrichment:
        return ""

    display = format_enrichment_for_display(enrichment)

    lines = [
        f"ARI: {display.get('ari_category', 'N/A')} ({display.get('ari_probability', 'N/A')}%)",
        f"Utilization: {display.get('utilization_rate', 'N/A')}",
        f"GMV Rate: {display.get('gmv_rate', 'N/A')}",
        f"Territory: {display.get('territory', 'N/A')}",
    ]

    return "\n".join(lines)


def get_account_attrition(account_id: str, cloud: str = "Commerce Cloud") -> list:
    """Get product-level attrition breakdown."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        query = f"""
        SELECT
            APM_LVL_3 as product,
            ABS(ATTRITION_PIPELINE) as attrition,
            ATTRITION_PROBA_CATEGORY as category
        FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE SF_ACCOUNT_ID = '{account_id}'
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.ATTRITION_PREDICTION_ACCT_PRODUCT
        )
        ORDER BY ABS(ATTRITION_PIPELINE) DESC
        """

        cursor.execute(query)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                "product": row[0],
                "attrition": float(row[1]) if row[1] else 0,
                "category": row[2],
            })
        return results

    except Exception as e:
        log_error(f"get_account_attrition error: {e}")
        return []
    finally:
        cursor.close()
