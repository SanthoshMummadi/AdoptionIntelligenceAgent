from domain.analytics.snowflake_client import run_query

account_id_15 = "00130000002xFEI"  # Adidas AG

print("=" * 60)
print("COMMERCE CLOUD UTILIZATION - Adidas AG")
print("=" * 60)

# Step 1: Check CURR_SNAP_FLG values
print("\n1. Check CURR_SNAP_FLG values:")
rows = run_query(
    """
    SELECT DISTINCT CURR_SNAP_FLG, SNAPSHOT_DT, COUNT(*) as CNT
    FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
    WHERE ACCOUNT_ID = %s
    GROUP BY CURR_SNAP_FLG, SNAPSHOT_DT
    ORDER BY SNAPSHOT_DT DESC
    LIMIT 5
""",
    [account_id_15],
)
for r in rows:
    print(
        f"  CURR_SNAP_FLG: {r.get('CURR_SNAP_FLG')} | "
        f"SNAPSHOT: {r.get('SNAPSHOT_DT')} | COUNT: {r.get('CNT')}"
    )

# Step 2: Commerce Cloud specific filter
print("\n2. Commerce Cloud usage (DRVD_APM_LVL_1 = Commerce):")
rows = run_query(
    """
    SELECT
        DRVD_APM_LVL_1,
        DRVD_APM_LVL_2,
        GRP,
        TYPE,
        PROVISIONED,
        ACTIVATED,
        USED,
        SNAPSHOT_DT,
        CURR_SNAP_FLG
    FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
    WHERE ACCOUNT_ID = %s
    AND DRVD_APM_LVL_1 LIKE '%%Commerce%%'
    AND SNAPSHOT_DT = (
        SELECT MAX(SNAPSHOT_DT)
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
        WHERE ACCOUNT_ID = %s
    )
    ORDER BY PROVISIONED DESC
""",
    [account_id_15, account_id_15],
)

if rows:
    print(f"  ✓ Found {len(rows)} Commerce rows:")
    total_prov = 0
    total_used = 0
    for r in rows:
        prov = float(r.get("PROVISIONED") or 0)
        used = float(r.get("USED") or 0)
        total_prov += prov
        total_used += used
        util = (used / prov * 100) if prov > 0 else 0
        print(f"  L1: {r.get('DRVD_APM_LVL_1')} | L2: {r.get('DRVD_APM_LVL_2')}")
        print(f"  GRP: {r.get('GRP')} | TYPE: {r.get('TYPE')}")
        print(f"  PROV: {prov:,.0f} | USED: {used:,.0f} | UTIL: {util:.1f}%")
        print()

    overall_util = (total_used / total_prov * 100) if total_prov > 0 else 0
    print(f"  OVERALL Commerce Utilization: {overall_util:.1f}%")
    print(f"  Total Provisioned: {total_prov:,.0f}")
    print(f"  Total Used: {total_used:,.0f}")
else:
    print("  ⚠️ No Commerce rows found")

# Step 3: GMV specifically (key metric!)
print("\n3. GMV specific usage:")
rows = run_query(
    """
    SELECT
        DRVD_APM_LVL_2,
        GRP,
        TYPE,
        PROVISIONED,
        USED,
        SNAPSHOT_DT
    FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
    WHERE ACCOUNT_ID = %s
    AND GRP = 'GMV'
    AND SNAPSHOT_DT = (
        SELECT MAX(SNAPSHOT_DT)
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
        WHERE ACCOUNT_ID = %s
    )
""",
    [account_id_15, account_id_15],
)

if rows:
    print(f"  ✓ Found {len(rows)} GMV row(s):")
    for r in rows:
        prov = float(r.get("PROVISIONED") or 0)
        used = float(r.get("USED") or 0)
        util = (used / prov * 100) if prov > 0 else 0
        print(f"  L2: {r.get('DRVD_APM_LVL_2')} | GRP: {r.get('GRP')}")
        print(f"  PROV: {prov:,.0f} | USED: {used:,.0f} | UTIL: {util:.1f}%")
else:
    print("  ⚠️ No GMV rows found")

print("\n" + "=" * 60)
