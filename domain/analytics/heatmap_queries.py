"""
domain/analytics/heatmap_queries.py
Snowflake queries for adoption heatmap visualization.
"""
from log_utils import log_debug, log_error

# Cloud mapping: (PRODUCT_PORTFOLIO, PRODUCT_FEATURE_FAMILY)
_CLOUD_MAPPING = {
    "Commerce B2B":       ("Commerce", "B2B Commerce"),
    "Commerce OMS":       ("Commerce", "Order Management"),
    "B2B Commerce":       ("Commerce", "B2B Commerce"),
    "Order Management":   ("Commerce", "Order Management"),
    "Financial Services": ("Industries", "Financial Services Cloud"),
    "Sales Cloud":        ("Sales Cloud", "Sales Cloud"),
    "Service Cloud":      ("Service Cloud", "Service Cloud Core"),
    "Agentforce":         ("Agentforce", "Build, Test, Observe"),
}


def get_fy_quarter_dates(fy: str) -> list[dict]:
    """
    Returns quarter date ranges for a Salesforce FY.
    Salesforce FY: Feb 1 - Jan 31
    Q1: Feb-Apr, Q2: May-Jul, Q3: Aug-Oct, Q4: Nov-Jan

    Args:
        fy: Fiscal year string (e.g., "FY27", "FY28")

    Returns:
        List of dicts with quarter labels and date ranges:
        [
            {"quarter": "Q1", "start": "2026-02-01", "end": "2026-04-30"},
            {"quarter": "Q2", "start": "2026-05-01", "end": "2026-07-31"},
            {"quarter": "Q3", "start": "2026-08-01", "end": "2026-10-31"},
            {"quarter": "Q4", "start": "2026-11-01", "end": "2027-01-31"},
        ]
    """
    # Parse FY number (e.g., "FY27" -> 27, "FY2027" -> 2027)
    fy_str = fy.replace("FY", "").strip()
    fy_num = int(fy_str)

    # Assume 2-digit year for 20XX
    if fy_num < 100:
        fy_year = 2000 + fy_num
    else:
        fy_year = fy_num

    start_year = fy_year - 1
    end_year = fy_year

    return [
        {
            "quarter": "Q1",
            "start": f"{start_year}-02-01",
            "end": f"{start_year}-04-30",
        },
        {
            "quarter": "Q2",
            "start": f"{start_year}-05-01",
            "end": f"{start_year}-07-31",
        },
        {
            "quarter": "Q3",
            "start": f"{start_year}-08-01",
            "end": f"{start_year}-10-31",
        },
        {
            "quarter": "Q4",
            "start": f"{start_year}-11-01",
            "end": f"{end_year}-01-31",
        },
    ]


def get_adoption_heatmap_data(cloud: str, fy: str = "FY2027") -> dict:
    """
    Returns adoption heatmap data for a given cloud and FY.

    Args:
        cloud: Cloud name (e.g., "Commerce B2B", "Sales Cloud")
        fy: Fiscal year (e.g., "FY2027", "FY2028")

    Returns:
        {
            "cloud": str,
            "fy": str,
            "quarters": {
                "Q1": [...],  # list of feature dicts
                "Q2": [...],
                "Q3": [...],
                "Q4": [...],
            },
            "summary": {
                "total_features": int,
                "green": int,
                "amber": int,
                "red": int,
                "latest_dt": str,
            }
        }

    Raises:
        ValueError: If cloud is not in _CLOUD_MAPPING
    """
    from domain.analytics.adoption_scoring import calculate_adoption_score, score_to_emoji

    # STEP B1 — Validate cloud input
    if cloud not in _CLOUD_MAPPING:
        raise ValueError(
            f"Unknown cloud: {cloud}. "
            f"Valid options: {list(_CLOUD_MAPPING.keys())}"
        )

    portfolio, family = _CLOUD_MAPPING[cloud]
    log_debug(f"[PDP] Fetching heatmap: {cloud} ({portfolio}/{family}) for {fy}")

    # STEP B2 — Get quarter date ranges
    quarters = get_fy_quarter_dates(fy)

    # STEP B3 — Query each quarter
    quarter_results = {}

    try:
        from domain.analytics.snowflake_client import (
            get_pdp_snowflake_connection,
            return_pdp_connection
        )
    except Exception as e:
        log_error(f"[PDP] Import failed: {e}")
        return {
            "cloud": cloud,
            "fy": fy,
            "quarters": {"Q1": [], "Q2": [], "Q3": [], "Q4": []},
            "summary": {
                "total_features": 0,
                "green": 0,
                "amber": 0,
                "red": 0,
                "latest_dt": "",
            }
        }

    conn = None
    try:
        conn = get_pdp_snowflake_connection()
    except Exception as e:
        log_error(f"[PDP] Connection failed: {e}")
        return {
            "cloud": cloud,
            "fy": fy,
            "quarters": {"Q1": [], "Q2": [], "Q3": [], "Q4": []},
            "summary": {
                "total_features": 0,
                "green": 0,
                "amber": 0,
                "red": 0,
                "latest_dt": "",
            }
        }

    for q_info in quarters:
        quarter_label = q_info["quarter"]
        snapshot_date = q_info["end"]
        total_accounts = 1

        query = """
        SELECT
            PRODUCT_FEATURE_GROUP,
            PRODUCT_FEATURE,
            PRODUCT_FEATURE_ID,
            PF_OWNER_NAME,
            PF_FEATURE_DESCRIPTION,
            PF_FEATURE_AVAILABILITY_STATUS,
            COUNT(DISTINCT ACCT_ID)                 AS account_count,
            COUNT(DISTINCT ORG_ID)                  AS org_count,
            SUM(ORG_PF_ROLLING_28D_MAU)             AS mau,
            SUM(ORG_PF_ROLLING_28D_TRANSACTIONS)    AS transactions,
            SUM(ORG_PF_EOM_MAU)                     AS eom_mau,
            MAX(DATA_DT)                            AS data_dt
        FROM DM_PRODUCT_PRD.GLD_ANALYTICS.RPT_PRODUCTUSAGE_PFT_ORG_METRICS
        WHERE PRODUCT_PORTFOLIO = %s
          AND PRODUCT_FEATURE_FAMILY = %s
          AND DATA_DT <= %s
          AND DATA_DT >= DATEADD(month, -1, %s)
          AND ORG_PF_ROLLING_28D_MAU > 0
        GROUP BY
            PRODUCT_FEATURE_GROUP, PRODUCT_FEATURE,
            PRODUCT_FEATURE_ID, PF_OWNER_NAME,
            PF_FEATURE_DESCRIPTION,
            PF_FEATURE_AVAILABILITY_STATUS
        ORDER BY PRODUCT_FEATURE_GROUP, mau DESC
        """

        try:
            total_cursor = conn.cursor()
            total_cursor.execute(
                """
                SELECT COUNT(DISTINCT ACCT_ID) AS total
                FROM DM_PRODUCT_PRD.GLD_ANALYTICS.RPT_PRODUCTUSAGE_PFT_ORG_METRICS
                WHERE PRODUCT_PORTFOLIO = %s
                  AND PRODUCT_FEATURE_FAMILY = %s
                  AND DATA_DT <= %s
                  AND DATA_DT >= DATEADD(month, -1, %s)
                """,
                (portfolio, family, snapshot_date, snapshot_date),
            )
            total_row = total_cursor.fetchone()
            total_accounts = int((total_row[0] if total_row else 0) or 1)
            total_cursor.close()

            cursor = conn.cursor()
            cursor.execute(query, (portfolio, family, snapshot_date, snapshot_date))
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                log_debug(f"[PDP] {quarter_label}: 0 features")
                quarter_results[quarter_label] = []
                continue

            # STEP B4 — Score each feature
            features = []
            for row in rows:
                # Get previous quarter account count for trend calculation
                prev_label = {"Q2": "Q1", "Q3": "Q2", "Q4": "Q3"}.get(quarter_label)
                prev_q_accounts = None
                if prev_label and prev_label in quarter_results:
                    # Find same feature in prev quarter
                    prev_match = next(
                        (f for f in quarter_results[prev_label]
                         if f["feature_id"] == row[2]),
                        None
                    )
                    prev_q_accounts = (
                        prev_match["account_count"] if prev_match else None
                    )

                score_data = calculate_adoption_score(
                    provisioned=total_accounts,
                    activated=int(row[6] or 0),        # ACCOUNT_COUNT
                    used=int(row[6] or 0),             # ACCOUNT_COUNT
                    account_count=int(row[6] or 0),    # ACCOUNT_COUNT
                    prev_quarter_used=prev_q_accounts
                )

                features.append({
                    "feature_group":   row[0] or "",
                    "feature":         str(row[1] or "").replace("|", "/"),
                    "feature_id":      row[2] or "",
                    "owner":           row[3] or "Unassigned",
                    "description":     row[4] or "",
                    "availability":    row[5] or "",
                    "status":          score_data["status"],
                    "emoji":           score_to_emoji(score_data["status"]),
                    "score":           score_data["score"],
                    "utilization":     score_data["utilization"],
                    "penetration":     score_data["penetration"],
                    "trend":           score_data["trend"],
                    "account_count":   int(row[6] or 0),
                    "org_count":       int(row[7] or 0),
                    "mau":             int(row[8] or 0),
                    "transactions":    int(row[9] or 0),
                    "data_dt":         str(row[11]) if row[11] else "",
                    "quarter":         f"{quarter_label} {fy}",
                })

            quarter_results[quarter_label] = features
            log_debug(f"[PDP] {quarter_label}: {len(features)} features")

        except Exception as e:
            log_error(f"[PDP] {quarter_label} query failed: {e}")
            quarter_results[quarter_label] = []

    # Close connection
    if conn:
        return_pdp_connection(conn)

    # STEP B5 — Build summary
    all_features = []
    for features in quarter_results.values():
        all_features.extend(features)

    unique_feature_ids = {f["feature_id"] for f in all_features if f["feature_id"]}
    green_count = sum(1 for f in all_features if f["status"] == "green")
    amber_count = sum(1 for f in all_features if f["status"] == "amber")
    red_count = sum(1 for f in all_features if f["status"] == "red")
    latest_dt = max((f["data_dt"] for f in all_features if f["data_dt"]), default="")

    # STEP B6 — Return
    return {
        "cloud": cloud,
        "fy": fy,
        "quarters": {
            "Q1": quarter_results.get("Q1", []),
            "Q2": quarter_results.get("Q2", []),
            "Q3": quarter_results.get("Q3", []),
            "Q4": quarter_results.get("Q4", []),
        },
        "summary": {
            "total_features": len(unique_feature_ids),
            "green": green_count,
            "amber": amber_count,
            "red": red_count,
            "latest_dt": latest_dt,
        }
    }


def get_feature_account_movers(
    feature_id: str,
    snapshot_date: str,
    portfolio: str,
    family: str,
    top_n: int = 5
) -> dict:
    """
    Returns top mover and top loser accounts for a given feature
    by comparing current vs prior month MAU.

    Args:
        feature_id:    PRODUCT_FEATURE_ID from RPT_PRODUCTUSAGE_PFT_ORG_METRICS
        snapshot_date: Current snapshot date string e.g. '2026-04-22'
        portfolio:     e.g. 'Commerce'
        family:        e.g. 'B2B Commerce'
        top_n:         Number of movers/losers to return (default 5)

    Returns:
        {
            "top_movers": [...],   # accounts with biggest MAU growth
            "top_losers": [...],   # accounts with biggest MAU drop
        }

    Each account dict:
        {
            "acct_nm":        str,
            "acct_id":        str,
            "csm_name":       str,
            "csg_region":     str,
            "mau_current":    int,
            "mau_prior":      int,
            "mau_change_pct": float,
        }
    """
    from domain.analytics.snowflake_client import (
        get_pdp_snowflake_connection,
        return_pdp_connection,
    )

    conn = None
    try:
        conn = get_pdp_snowflake_connection()
        cur = conn.cursor()

        # Get prior snapshot date — latest date before current
        cur.execute("""
            SELECT MAX(DATA_DT)
            FROM DM_PRODUCT_PRD.GLD_ANALYTICS.RPT_PRODUCTUSAGE_PFT_ORG_METRICS
            WHERE PRODUCT_PORTFOLIO    = %s
              AND PRODUCT_FEATURE_FAMILY = %s
              AND DATA_DT < %s
        """, (portfolio, family, snapshot_date))
        row = cur.fetchone()
        prior_date = str(row[0]) if row and row[0] else None

        if not prior_date:
            return {"top_movers": [], "top_losers": []}

        # Query current + prior MAU per account for this feature
        cur.execute("""
            WITH current_snap AS (
                SELECT
                    ACCT_ID,
                    MAX(ACCT_NM)                        AS acct_nm,
                    MAX(CSM_NAME)                       AS csm_name,
                    MAX(CSG_REGION_NM)                  AS csg_region,
                    SUM(ORG_PF_ROLLING_28D_MAU)         AS mau_current
                FROM DM_PRODUCT_PRD.GLD_ANALYTICS.RPT_PRODUCTUSAGE_PFT_ORG_METRICS
                WHERE PRODUCT_FEATURE_ID    = %s
                  AND DATA_DT               = %s
                  AND ORG_PF_ROLLING_28D_MAU > 10
                GROUP BY ACCT_ID
            ),
            prior_snap AS (
                SELECT
                    ACCT_ID,
                    SUM(ORG_PF_ROLLING_28D_MAU)         AS mau_prior
                FROM DM_PRODUCT_PRD.GLD_ANALYTICS.RPT_PRODUCTUSAGE_PFT_ORG_METRICS
                WHERE PRODUCT_FEATURE_ID    = %s
                  AND DATA_DT               = %s
                  AND ORG_PF_ROLLING_28D_MAU > 10
                GROUP BY ACCT_ID
            )
            SELECT
                c.ACCT_ID,
                c.acct_nm,
                c.csm_name,
                c.csg_region,
                c.mau_current,
                p.mau_prior,
                ROUND(
                    (c.mau_current - p.mau_prior)
                    / NULLIF(p.mau_prior, 0) * 100
                , 1)                                    AS mau_change_pct
            FROM current_snap c
            JOIN prior_snap p ON c.ACCT_ID = p.ACCT_ID
            WHERE p.mau_prior > 0
              AND c.mau_current > 0
            ORDER BY mau_change_pct DESC
        """, (feature_id, snapshot_date, feature_id, prior_date))

        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        cur.close()

        all_accounts = []
        for row in rows:
            d = dict(zip(cols, row))
            all_accounts.append({
                "acct_nm":        str(d["ACCT_NM"] or ""),
                "acct_id":        str(d["ACCT_ID"] or ""),
                "csm_name":       str(d["CSM_NAME"] or "Unassigned"),
                "csg_region":     str(d["CSG_REGION"] or ""),
                "mau_current":    int(d["MAU_CURRENT"] or 0),
                "mau_prior":      int(d["MAU_PRIOR"] or 0),
                "mau_change_pct": float(d["MAU_CHANGE_PCT"] or 0.0),
            })

        # Split into movers and losers
        top_movers = [a for a in all_accounts if a["mau_change_pct"] > 0][:top_n]
        top_losers = sorted(
            [a for a in all_accounts if a["mau_change_pct"] < 0],
            key=lambda x: x["mau_change_pct"]
        )[:top_n]

        return {
            "top_movers": top_movers,
            "top_losers": top_losers,
        }

    except Exception as e:
        import logging
        logging.getLogger(__name__).error(
            f"[PDP] get_feature_account_movers failed: {e}"
        )
        return {"top_movers": [], "top_losers": []}

    finally:
        if conn:
            return_pdp_connection(conn)
