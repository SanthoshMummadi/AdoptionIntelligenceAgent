from domain.analytics.snowflake_client import run_query

account_id_15 = "00130000002xFEI"  # Adidas AG

print("=" * 60)
print("TESTING UTILIZATION DATA SOURCES")
print("=" * 60)

# Test 1: CIDM.CI_FACT_TENANT_ENT_USG_MTHLY
print("\n1. Testing CIDM.CI_FACT_TENANT_ENT_USG_MTHLY...")
try:
    rows = run_query(
        """
        SELECT ACCOUNT_ID, SUBCLOUD, TOTAL_USAGE, PRECOMMIT_QTY, SNAPSHOT_DT
        FROM SSE_DM_CSG_RPT_PRD.CIDM.CI_FACT_TENANT_ENT_USG_MTHLY
        WHERE ACCOUNT_ID = %s
        AND SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM SSE_DM_CSG_RPT_PRD.CIDM.CI_FACT_TENANT_ENT_USG_MTHLY)
        LIMIT 5
    """,
        [account_id_15],
    )
    if rows:
        print(f"  ✓ Found {len(rows)} rows!")
        for r in rows:
            print(
                f"    SUBCLOUD: {r.get('SUBCLOUD')} | USAGE: {r.get('TOTAL_USAGE')} | PRECOMMIT: {r.get('PRECOMMIT_QTY')}"
            )
    else:
        print("  ⚠️ No rows found")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:100]}")

# Test 2: CIDM.WV_AV_USAGE_EXTRACT_VW
print("\n2. Testing CIDM.WV_AV_USAGE_EXTRACT_VW...")
try:
    rows = run_query(
        """
        SELECT ACCOUNT_ID, DRVD_APM_LVL_2, TYPE, PROVISIONED, ACTUAL_USAGE, PERC_USAGE
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
        WHERE ACCOUNT_ID = %s
        LIMIT 5
    """,
        [account_id_15],
    )
    if rows:
        print(f"  ✓ Found {len(rows)} rows!")
        for r in rows:
            print(
                f"    L2: {r.get('DRVD_APM_LVL_2')} | PROV: {r.get('PROVISIONED')} | USAGE: {r.get('ACTUAL_USAGE')} | %: {r.get('PERC_USAGE')}"
            )
    else:
        print("  ⚠️ No rows found")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:100]}")

# Test 3: CIDM.CI_FACT_AOV_ACCOUNT
print("\n3. Testing CIDM.CI_FACT_AOV_ACCOUNT...")
try:
    rows = run_query(
        """
        SELECT ACCOUNT_ID, APM_L1, APM_L2, BEGIN_AOV, SNAPSHOT_DT
        FROM SSE_DM_CSG_RPT_PRD.CIDM.CI_FACT_AOV_ACCOUNT
        WHERE ACCOUNT_ID = %s
        AND SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM SSE_DM_CSG_RPT_PRD.CIDM.CI_FACT_AOV_ACCOUNT)
        AND APM_L1 LIKE '%%Commerce%%'
        LIMIT 5
    """,
        [account_id_15],
    )
    if rows:
        print(f"  ✓ Found {len(rows)} rows!")
        for r in rows:
            aov = r.get("BEGIN_AOV", 0) or 0
            try:
                aov_s = f"${float(aov):,.0f}"
            except (TypeError, ValueError):
                aov_s = str(aov)
            print(f"    L1: {r.get('APM_L1')} | L2: {r.get('APM_L2')} | AOV: {aov_s}")
    else:
        print("  ⚠️ No rows found")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:100]}")

# Test 4: CI_EWS360_SCORES
print("\n4. Testing CI_DS_OUT.CI_EWS360_SCORES...")
try:
    rows = run_query(
        """
        SELECT ACCOUNT_ID, SUBCLOUD, EWS360_SCORE, ATTRITION_SCORE, SNAPSHOT_DT
        FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.CI_EWS360_SCORES
        WHERE ACCOUNT_ID = %s
        AND SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.CI_EWS360_SCORES)
        LIMIT 5
    """,
        [account_id_15],
    )
    if rows:
        print(f"  ✓ Found {len(rows)} rows!")
        for r in rows:
            print(
                f"    SUBCLOUD: {r.get('SUBCLOUD')} | EWS360: {r.get('EWS360_SCORE')} | ATTRITION: {r.get('ATTRITION_SCORE')}"
            )
    else:
        print("  ⚠️ No rows found")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:100]}")

print("\n" + "=" * 60)
