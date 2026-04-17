"""
Bulk CIDM usage queries.
Single query returns usage data for ALL accounts at once.
"""
import os
from collections import defaultdict

from domain.analytics.snowflake_client import run_query


def get_usage_bulk(account_ids: list[str], cloud: str = None) -> dict:
    """
    Single CIDM query for all accounts.
    Returns {account_id_15: usage_data} where usage_data includes:
      - utilization_rate
      - provisioned / used
      - products
      - raw_rows       (cloud-filtered, for Adoption POV)
      - all_raw_rows   (unfiltered, for SF Products)
    """
    if not account_ids:
        return {}

    ids_15 = [aid[:15] for aid in account_ids if aid]
    ids_sql = "','".join(ids_15)
    snapshot_date = os.getenv("SNOWFLAKE_CIDM_SNAPSHOT_DT", "2026-04-01")

    # Fetch ALL rows first (no cloud filter)
    all_rows = run_query(f"""
        SELECT
            ACCOUNT_ID,
            DRVD_APM_LVL_1,
            DRVD_APM_LVL_2,
            GRP,
            TYPE,
            PROVISIONED,
            ACTIVATED,
            USED
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
        WHERE ACCOUNT_ID IN ('{ids_sql}')
        AND CURR_SNAP_FLG = 'Y'
        AND SNAPSHOT_DT = '{snapshot_date}'
        AND PROVISIONED > 0
    """)

    # Apply cloud filter in Python for filtered rows
    def _matches_cloud(row: dict, cloud_name: str) -> bool:
        if not cloud_name:
            return True
        c = cloud_name.lower()
        l1 = str(row.get("DRVD_APM_LVL_1") or "").lower()
        l2 = str(row.get("DRVD_APM_LVL_2") or "").lower()
        if "financial services" in c or c == "fsc":
            return "financial services" in l2 or "industries" in l1
        if "commerce" in c:
            return "commerce" in l1 or "commerce" in l2
        if "marketing" in c:
            return "marketing" in l1 or "marketing" in l2
        if "tableau" in c:
            return "tableau" in l1 or "tableau" in l2
        if "mulesoft" in c or "integration" in c:
            return "integration" in l1 or "mulesoft" in l1
        if "sales" in c:
            return "sales" in l1 or "sales" in l2
        if "service" in c:
            return "service" in l1 or "service" in l2
        return True

    # Group all rows by account
    all_account_rows = defaultdict(list)
    for r in all_rows:
        all_account_rows[r["ACCOUNT_ID"]].append(r)

    # Group cloud-filtered rows by account
    filtered_account_rows = defaultdict(list)
    for r in all_rows:
        if _matches_cloud(r, cloud):
            filtered_account_rows[r["ACCOUNT_ID"]].append(r)

    # Build usage map
    result = {}
    for acct_id, acct_all_rows in all_account_rows.items():
        filtered_rows = filtered_account_rows.get(acct_id, acct_all_rows)

        # Utilization from filtered rows (cloud-specific)
        total_prov = sum(float(r.get("PROVISIONED") or 0) for r in filtered_rows)
        total_used = sum(float(r.get("USED") or 0) for r in filtered_rows)
        util = (total_used / total_prov * 100) if total_prov > 0 else 0

        # SF Products from ALL rows (breadth view)
        apm_l1_all = list(dict.fromkeys(
            str(r.get("DRVD_APM_LVL_1") or "").strip()
            for r in acct_all_rows
            if str(r.get("DRVD_APM_LVL_1") or "").strip()
            and str(r.get("DRVD_APM_LVL_1") or "").strip() not in ("Other", "")
        ))

        result[acct_id] = {
            "utilization_rate": f"{util:.1f}%",
            "provisioned": total_prov,
            "used": total_used,
            "products": apm_l1_all,
            "sf_products": ", ".join(apm_l1_all),
            "raw_rows": filtered_rows,      # cloud-filtered -> Adoption POV
            "all_raw_rows": acct_all_rows,  # unfiltered -> SF Products
        }

    return result
