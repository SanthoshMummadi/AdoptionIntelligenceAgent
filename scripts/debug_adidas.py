from domain.analytics.snowflake_client import run_query

opp_id = "0063y00001ANfq2AAD"
account_id = "00130000002xFEIAA2"

print("=" * 70)
print("DEBUGGING - Checking Snowflake for Adidas AG")
print("=" * 70)

# Check 1: Does this opp exist in ATTRITION_PREDICTION_OPPTY?
print("1. Checking CSS.ATTRITION_PREDICTION_OPPTY...")
rows = run_query(
    """
    SELECT RENEWAL_OPTY_ID, ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY, SNAPSHOT_DT
    FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_OPPTY
    WHERE RENEWAL_OPTY_ID = %s
    ORDER BY SNAPSHOT_DT DESC
    LIMIT 5
""",
    [opp_id],
)

if rows:
    print(f"   Found {len(rows)} record(s):")
    for r in rows:
        print(
            f"     - Snapshot: {r.get('SNAPSHOT_DT')}, ARI: "
            f"{r.get('ATTRITION_PROBA_CATEGORY')} ({r.get('ATTRITION_PROBA')})"
        )
else:
    print("   No records found for this opp ID")

print()

# Check 2: Does this account exist in ATTRITION_PREDICTION_ACCT_PRODUCT?
print("2. Checking CSS.ATTRITION_PREDICTION_ACCT_PRODUCT...")
account_id_15 = account_id[:15]
rows = run_query(
    """
    SELECT ACCOUNT_ID, APM_LVL_3, ATTRITION_PROBA_CATEGORY, ATTRITION_PIPELINE, SNAPSHOT_DT
    FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
    WHERE ACCOUNT_ID = %s
    ORDER BY SNAPSHOT_DT DESC, ATTRITION_PIPELINE DESC
    LIMIT 5
""",
    [account_id_15],
)

if rows:
    print(f"   Found {len(rows)} product(s):")
    for r in rows:
        atr = abs(float(r.get("ATTRITION_PIPELINE") or 0))
        print(
            f"     - {r.get('APM_LVL_3')}: {r.get('ATTRITION_PROBA_CATEGORY')} (${atr:,.0f})"
        )
else:
    print("   No records found for this account ID")

print()

# Check 3: Does this opp exist in RENEWALS.WV_CI_RENEWAL_OPTY_VW?
print("3. Checking RENEWALS.WV_CI_RENEWAL_OPTY_VW...")
rows = run_query(
    """
    SELECT RENEWAL_OPTY_ID, ACCOUNT_NM, TARGET_CLOUD,
           RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV, RENEWAL_FCAST_ATTRITION_CONV
    FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW
    WHERE RENEWAL_OPTY_ID = %s
    LIMIT 1
""",
    [opp_id],
)

if rows:
    r = rows[0]
    print("   Found renewal data:")
    print(f"     - Account: {r.get('ACCOUNT_NM')}")
    print(f"     - Cloud: {r.get('TARGET_CLOUD')}")
    conv = float(r.get("RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV") or 0)
    fcast = float(r.get("RENEWAL_FCAST_ATTRITION_CONV") or 0)
    print(f"     - AOV: ${conv:,.0f}")
    print(f"     - ATR: ${abs(fcast):,.0f}")
else:
    print("   No records found for this opp ID")

print("=" * 70)
