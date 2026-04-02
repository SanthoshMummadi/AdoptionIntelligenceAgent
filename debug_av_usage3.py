from domain.analytics.snowflake_client import run_query
from domain.salesforce.org62_client import resolve_account

account_id_15 = "00130000002xFEI"

print("=" * 60)
print("DISTINCT L1/L2/GRP for Adidas AG - Latest Snapshot")
print("=" * 60)

# Check what L1/L2/GRP values exist for this account
print("\n1. All distinct L1/L2/GRP at CURR_SNAP_FLG = Y:")
rows = run_query(
    """
    SELECT
        DRVD_APM_LVL_1,
        DRVD_APM_LVL_2,
        GRP,
        TYPE,
        SUM(PROVISIONED) as TOTAL_PROV,
        SUM(USED) as TOTAL_USED,
        SNAPSHOT_DT
    FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
    WHERE ACCOUNT_ID = %s
    AND CURR_SNAP_FLG = 'Y'
    GROUP BY DRVD_APM_LVL_1, DRVD_APM_LVL_2, GRP, TYPE, SNAPSHOT_DT
    ORDER BY TOTAL_PROV DESC
""",
    [account_id_15],
)

if rows:
    print(f"  ✓ Found {len(rows)} distinct rows at CURR_SNAP=Y:")
    for r in rows:
        prov = float(r.get("TOTAL_PROV") or 0)
        used = float(r.get("TOTAL_USED") or 0)
        util = (used / prov * 100) if prov > 0 else 0
        print(f"  L1: {r.get('DRVD_APM_LVL_1')} | L2: {r.get('DRVD_APM_LVL_2')}")
        print(f"  GRP: {r.get('GRP')} | TYPE: {r.get('TYPE')}")
        print(f"  PROV: {prov:,.0f} | USED: {used:,.0f} | UTIL: {util:.1f}%")
        print()
else:
    print("  ⚠️ No rows at CURR_SNAP_FLG = Y")

print("\n2. Check GMV rows specifically (any snapshot):")
rows = run_query(
    """
    SELECT
        DRVD_APM_LVL_1, DRVD_APM_LVL_2,
        GRP, TYPE,
        PROVISIONED, USED, SNAPSHOT_DT, CURR_SNAP_FLG
    FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
    WHERE ACCOUNT_ID = %s
    AND CURR_SNAP_FLG = 'Y'
    AND PROVISIONED > 0
    ORDER BY PROVISIONED DESC
    LIMIT 10
""",
    [account_id_15],
)

if rows:
    print(f"  ✓ Found {len(rows)} rows with PROVISIONED > 0:")
    for r in rows:
        prov = float(r.get("PROVISIONED") or 0)
        used = float(r.get("USED") or 0)
        util = (used / prov * 100) if prov > 0 else 0
        print(f"  L1: {r.get('DRVD_APM_LVL_1')} | L2: {r.get('DRVD_APM_LVL_2')}")
        print(f"  GRP: {r.get('GRP')} | TYPE: {r.get('TYPE')}")
        print(f"  PROV: {prov:,.0f} | USED: {used:,.0f} | UTIL: {util:.1f}%")
        print()
else:
    print("  ⚠️ No rows with PROVISIONED > 0 at CURR_SNAP=Y")

# Now try LVMH to understand the GMV structure
print("\n3. Check LVMH for Commerce/GMV structure (reference account):")
acct = resolve_account("LVMH")
if acct:
    lvmh_id_15 = acct["id"][:15]
    print(f"  LVMH Account ID (15): {lvmh_id_15}")

    rows = run_query(
        """
        SELECT
            DRVD_APM_LVL_1, DRVD_APM_LVL_2,
            GRP, TYPE,
            SUM(PROVISIONED) as TOTAL_PROV,
            SUM(USED) as TOTAL_USED,
            SNAPSHOT_DT
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
        WHERE ACCOUNT_ID = %s
        AND CURR_SNAP_FLG = 'Y'
        AND PROVISIONED > 0
        GROUP BY DRVD_APM_LVL_1, DRVD_APM_LVL_2, GRP, TYPE, SNAPSHOT_DT
        ORDER BY TOTAL_PROV DESC
        LIMIT 10
    """,
        [lvmh_id_15],
    )

    if rows:
        print(f"  ✓ Found {len(rows)} LVMH rows with PROVISIONED > 0:")
        for r in rows:
            prov = float(r.get("TOTAL_PROV") or 0)
            used = float(r.get("TOTAL_USED") or 0)
            util = (used / prov * 100) if prov > 0 else 0
            print(f"  L1: {r.get('DRVD_APM_LVL_1')} | L2: {r.get('DRVD_APM_LVL_2')}")
            print(f"  GRP: {r.get('GRP')} | TYPE: {r.get('TYPE')}")
            print(f"  PROV: {prov:,.0f} | USED: {used:,.0f} | UTIL: {util:.1f}%")
            print()
    else:
        print("  ⚠️ No LVMH rows found")
else:
    print("  ⚠️ LVMH not found in Salesforce")

print("\n" + "=" * 60)
