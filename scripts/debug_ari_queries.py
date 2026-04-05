"""One-off: compare account-level ARI with vs without Commerce Cloud filter (debug)."""
import sys

from domain.analytics.snowflake_client import run_query

account_id_15 = (sys.argv[1] if len(sys.argv) > 1 else "00130000002xFEI").strip()


def main() -> None:
    print("Testing ARI queries for ACCOUNT_ID =", account_id_15)
    print()

    print("1. Account-level ARI (NO cloud filter):")
    rows = run_query(
        """
        SELECT ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY, ATTRITION_REASON,
               APM_LVL_2, APM_LVL_3
        FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE ACCOUNT_ID = %s
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        )
        ORDER BY ATTRITION_PROBA DESC NULLS LAST
        LIMIT 10
        """,
        [account_id_15],
    )
    if rows:
        print(f"   ✓ Found {len(rows)} product row(s):")
        for r in rows:
            prob = r.get("ATTRITION_PROBA")
            try:
                prob_s = f"{float(prob):.2f}"
            except (TypeError, ValueError):
                prob_s = str(prob)
            name = r.get("APM_LVL_3") or r.get("APM_LVL_2")
            print(f"     - {name}: {r.get('ATTRITION_PROBA_CATEGORY')} ({prob_s})")
    else:
        print("   ❌ No data found")
    print()

    print("2. Account-level ARI (WITH Commerce Cloud filter):")
    rows_cc = run_query(
        """
        SELECT ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY, ATTRITION_REASON,
               APM_LVL_2, APM_LVL_3
        FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE ACCOUNT_ID = %s
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        )
        AND (
            APM_LVL_1 LIKE '%%Commerce%%' OR APM_LVL_2 LIKE '%%Commerce%%'
            OR APM_LVL_3 LIKE '%%Commerce%%'
        )
        ORDER BY ATTRITION_PROBA DESC NULLS LAST
        LIMIT 10
        """,
        [account_id_15],
    )
    if rows_cc:
        print(f"   ✓ Found {len(rows_cc)} Commerce row(s):")
        for r in rows_cc:
            prob = r.get("ATTRITION_PROBA")
            try:
                prob_s = f"{float(prob):.2f}"
            except (TypeError, ValueError):
                prob_s = str(prob)
            name = r.get("APM_LVL_3") or r.get("APM_LVL_2")
            print(f"     - {name}: {r.get('ATTRITION_PROBA_CATEGORY')} ({prob_s})")
    else:
        print("   ❌ No Commerce Cloud data found")


if __name__ == "__main__":
    main()
