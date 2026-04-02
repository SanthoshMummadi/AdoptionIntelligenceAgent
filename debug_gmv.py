from domain.analytics.snowflake_client import run_query
from domain.salesforce.org62_client import resolve_account

account_id_15 = "0010M00001NYVup"  # Oxford Industries - get this from trace

acct = resolve_account("Oxford Industries")
if acct:
    account_id_15 = acct["id"][:15]
    print(f"Oxford Industries Account ID (15): {account_id_15}")

print()
print("=" * 60)
print("GMV RAW DATA - Oxford Industries")
print("=" * 60)

print("\n1. GMV rows at CURR_SNAP_FLG = Y:")
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
    AND CURR_SNAP_FLG = 'Y'
    AND GRP = 'GMV'
    ORDER BY PROVISIONED DESC
""",
    [account_id_15],
)

if rows:
    print(f"  ✓ Found {len(rows)} GMV row(s):")
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
        print(f"  PROV: {prov:,.0f} | USED: {used:,.0f} | ROW UTIL: {util:.1f}%")
        print(
            f"  SNAPSHOT: {r.get('SNAPSHOT_DT')} | CURR_SNAP: {r.get('CURR_SNAP_FLG')}"
        )
        print()

    overall = (total_used / total_prov * 100) if total_prov > 0 else 0
    print(f"  TOTAL PROV: {total_prov:,.0f}")
    print(f"  TOTAL USED: {total_used:,.0f}")
    print(f"  CALCULATED UTIL: {overall:.1f}%")
    print(f"  EXPECTED UTIL: 50%")
    print(f"  DIFF: {overall - 50:.1f}%")
else:
    print("  ⚠️ No GMV rows at CURR_SNAP_FLG = Y")

print("\n2. GMV rows across ALL snapshots:")
rows = run_query(
    """
    SELECT
        SNAPSHOT_DT,
        CURR_SNAP_FLG,
        GRP,
        TYPE,
        SUM(PROVISIONED) as TOTAL_PROV,
        SUM(USED) as TOTAL_USED
    FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
    WHERE ACCOUNT_ID = %s
    AND GRP = 'GMV'
    GROUP BY SNAPSHOT_DT, CURR_SNAP_FLG, GRP, TYPE
    ORDER BY SNAPSHOT_DT DESC
    LIMIT 5
""",
    [account_id_15],
)

if rows:
    for r in rows:
        prov = float(r.get("TOTAL_PROV") or 0)
        used = float(r.get("TOTAL_USED") or 0)
        util = (used / prov * 100) if prov > 0 else 0
        print(
            f"  SNAP: {r.get('SNAPSHOT_DT')} | CURR: {r.get('CURR_SNAP_FLG')} | "
            f"PROV: {prov:,.0f} | USED: {used:,.0f} | UTIL: {util:.1f}%"
        )

print("\n3. Sandbox rows (might be included in dashboard):")
rows = run_query(
    """
    SELECT
        DRVD_APM_LVL_2, GRP, TYPE,
        SUM(PROVISIONED) as TOTAL_PROV,
        SUM(USED) as TOTAL_USED
    FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
    WHERE ACCOUNT_ID = %s
    AND CURR_SNAP_FLG = 'Y'
    AND DRVD_APM_LVL_1 LIKE '%%Commerce%%'
    GROUP BY DRVD_APM_LVL_2, GRP, TYPE
    ORDER BY TOTAL_PROV DESC
""",
    [account_id_15],
)

if rows:
    print(f"  ✓ Found {len(rows)} Commerce row(s):")
    total_prov = 0
    total_used = 0
    for r in rows:
        prov = float(r.get("TOTAL_PROV") or 0)
        used = float(r.get("TOTAL_USED") or 0)
        total_prov += prov
        total_used += used
        util = (used / prov * 100) if prov > 0 else 0
        print(
            f"  L2: {r.get('DRVD_APM_LVL_2')} | GRP: {r.get('GRP')} | TYPE: {r.get('TYPE')}"
        )
        print(f"  PROV: {prov:,.0f} | USED: {used:,.0f} | UTIL: {util:.1f}%")
        print()

    overall = (total_used / total_prov * 100) if total_prov > 0 else 0
    print(f"  COMBINED Commerce UTIL: {overall:.1f}%")
