from domain.analytics.snowflake_client import run_query

account_id_15 = "00130000002xFEI"

print("=== CI_EWS360_SCORES Columns ===")
rows = run_query(
    """
    SELECT COLUMN_NAME
    FROM SSE_DM_CSG_RPT_PRD.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'CI_DS_OUT'
    AND TABLE_NAME = 'CI_EWS360_SCORES'
    ORDER BY ORDINAL_POSITION
"""
)
for r in rows:
    print(" -", list(r.values())[0])

print()
print("=== Sample Data for Adidas AG ===")
rows = run_query(
    """
    SELECT *
    FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.CI_EWS360_SCORES
    WHERE ACCOUNT_ID = %s
    AND SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM SSE_DM_CSG_RPT_PRD.CI_DS_OUT.CI_EWS360_SCORES)
    ORDER BY SUBCLOUD
""",
    [account_id_15],
)

print(f"Found {len(rows)} rows")
for r in rows:
    print()
    for k, v in r.items():
        if v is not None:
            print(f"  {k}: {v}")
