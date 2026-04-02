"""
snowflake_client.py
Snowflake analytics client for Commerce Cloud data.
"""
import os
from typing import Any, Optional

import snowflake.connector
from log_utils import log_debug, log_error

_snowflake_conn = None


def get_snowflake_connection():
    """Get or create Snowflake connection (browser SSO via externalbrowser)."""
    global _snowflake_conn

    if _snowflake_conn is None:
        user = os.getenv("SNOWFLAKE_USER")
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE") or "COMPUTE_WH"
        database = os.getenv("SNOWFLAKE_DATABASE")
        schema = os.getenv("SNOWFLAKE_SCHEMA")
        role = os.getenv("SNOWFLAKE_ROLE")

        if not account or not user:
            raise Exception("Missing SNOWFLAKE_ACCOUNT or SNOWFLAKE_USER in .env")

        # Triggers browser SSO on first connect (run scripts from a machine with a display / browser).
        conn_params = {
            "user": user,
            "account": account,
            "authenticator": "externalbrowser",
            "warehouse": warehouse,
        }
        if database:
            conn_params["database"] = database
        if schema:
            conn_params["schema"] = schema
        if role:
            conn_params["role"] = role

        _snowflake_conn = snowflake.connector.connect(**conn_params)
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
        FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE UPPER(ACCOUNT_NAME) LIKE '%{account_name.upper()}%'
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
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
            ATTRITION_PROBA * 100 as probability,
            ATTRITION_REASON as reason,
            FACTORS_INCR_RISK as factors_increasing,
            FACTORS_DECR_RISK as factors_decreasing,
            RISK_DESCRIPTION as description
        FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE ACCOUNT_ID = '{account_id}'
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        )
        ORDER BY ATTRITION_PROBA DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        results = []
        for row in rows:
            results.append({
                "product": row[0],
                "category": row[1],
                "probability": round(float(row[2]), 1) if row[2] else 0,
                "reason": row[3] or "",
                "factors_incr": row[4] or "",
                "factors_decr": row[5] or "",
                "description": row[6] or "",
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
            "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT)"
        ]

        if min_attrition > 0:
            where_clauses.append(f"ABS(ATTRITION_PIPELINE) >= {min_attrition}")

        if ari_filter:
            where_clauses.append(f"ATTRITION_PROBA_CATEGORY = '{ari_filter}'")

        where_sql = " AND ".join(where_clauses)

        sort_map = {
            "atr": "ABS(ATTRITION_PIPELINE) DESC",
            "ari": "ATTRITION_PROBA DESC",
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
        FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
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
            aov.TOTALAOV as cloud_aov,
            SUM(usage.TOTAL_USAGE) as total_usage,
            SUM(usage.PRECOMMIT_QTY) as total_provisioned,
            CASE
                WHEN SUM(usage.PRECOMMIT_QTY) > 0
                THEN (SUM(usage.TOTAL_USAGE) / SUM(usage.PRECOMMIT_QTY)) * 100
                ELSE 0
            END as utilization_rate
        FROM CIDM.CI_FACT_TENANT_ENT_USG_MTHLY usage
        LEFT JOIN CIDM.CI_FACT_AOV_ACCOUNT aov
            ON usage.ACCOUNT_ID = aov.ACCOUNT_ID
        WHERE usage.ACCOUNT_ID = '{account_id}'
        AND usage.CURR_SNAP = 1
        GROUP BY aov.TOTALAOV
        LIMIT 1
        """

        cursor.execute(query)
        row = cursor.fetchone()
        cursor.close()

        usage = {}
        if row:
            usage = {
                "cc_aov": f"${float(row[0]):,.0f}" if row[0] else "N/A",
                "total_usage": float(row[1]) if row[1] else 0,
                "total_provisioned": float(row[2]) if row[2] else 0,
                "utilization_rate": (
                    f"{float(row[3]):.1f}%"
                    if row[3] is not None
                    else "N/A"
                ),
                "gmv_rate": "N/A",
                "burn_rate": "N/A",
                "territory": "N/A",
                "csg_geo": "N/A",
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
        FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE ACCOUNT_ID = '{account_id}'
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
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


def _escape_sf_id(account_id: str) -> str:
    """Sanitize account id for SQL string literals."""
    return str(account_id).replace("'", "")


class SnowflakeClient:
    """OOP wrapper over a dedicated Snowflake connection (for adapters / tests)."""

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
        """Latest ACCOUNT_METRICS row for account."""
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
        """Top ARI probability (0–100) for account, if any."""
        aid = _escape_sf_id(account_id)
        cursor = self._cursor()
        try:
            query = f"""
            SELECT ATTRITION_PROBA * 100 AS probability
            FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE ACCOUNT_ID = '{aid}'
            AND SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
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
        """Product-level attrition rows and summary for account."""
        aid = _escape_sf_id(account_id)
        cursor = self._cursor()
        try:
            query = f"""
            SELECT
                APM_LVL_3 AS product,
                ABS(ATTRITION_PIPELINE) AS attrition,
                ATTRITION_PROBA_CATEGORY AS category
            FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE ACCOUNT_ID = '{aid}'
            AND SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
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
