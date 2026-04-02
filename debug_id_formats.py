from domain.analytics.snowflake_client import run_query, to_15_char_id

opp_id_18 = "0063y00001ANfq2AAD"
opp_id_15 = to_15_char_id(opp_id_18)

print("=" * 70)
print("Testing ID formats: 18-char vs 15-char")
print("=" * 70)
print(f"18-char ID: {opp_id_18}")
print(f"15-char ID: {opp_id_15}")
print()

# Test 1: Try 18-char in ATTRITION_PREDICTION_OPPTY
print("1. CSS.ATTRITION_PREDICTION_OPPTY with 18-char ID:")
rows = run_query(
    """
    SELECT RENEWAL_OPTY_ID, ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY
    FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_OPPTY
    WHERE RENEWAL_OPTY_ID = %s
    ORDER BY SNAPSHOT_DT DESC
    LIMIT 1
""",
    [opp_id_18],
)
print(f"   Result: {len(rows)} records")

# Test 2: Try 15-char in ATTRITION_PREDICTION_OPPTY
print("2. CSS.ATTRITION_PREDICTION_OPPTY with 15-char ID:")
rows = run_query(
    """
    SELECT RENEWAL_OPTY_ID, ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY
    FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_OPPTY
    WHERE RENEWAL_OPTY_ID = %s
    ORDER BY SNAPSHOT_DT DESC
    LIMIT 1
""",
    [opp_id_15],
)
if rows:
    print(
        f"   Found with 15-char! ARI: {rows[0].get('ATTRITION_PROBA_CATEGORY')} "
        f"({rows[0].get('ATTRITION_PROBA')})"
    )
else:
    print(f"   Result: {len(rows)} records")

print()

# Test 3: Try 18-char in RENEWALS view
print("3. RENEWALS.WV_CI_RENEWAL_OPTY_VW with 18-char ID:")
rows = run_query(
    """
    SELECT RENEWAL_OPTY_ID, ACCOUNT_NM, RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV
    FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW
    WHERE RENEWAL_OPTY_ID = %s
    LIMIT 1
""",
    [opp_id_18],
)
print(f"   Result: {len(rows)} records")

# Test 4: Try 15-char in RENEWALS view
print("4. RENEWALS.WV_CI_RENEWAL_OPTY_VW with 15-char ID:")
rows = run_query(
    """
    SELECT RENEWAL_OPTY_ID, ACCOUNT_NM, RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV
    FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW
    WHERE RENEWAL_OPTY_ID = %s
    LIMIT 1
""",
    [opp_id_15],
)
if rows:
    aov = float(rows[0].get("RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV") or 0)
    print(
        f"   Found with 15-char! Account: {rows[0].get('ACCOUNT_NM')}, "
        f"AOV: ${aov:,.0f}"
    )
else:
    print(f"   Result: {len(rows)} records")

print("=" * 70)
