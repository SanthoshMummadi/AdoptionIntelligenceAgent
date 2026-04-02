from domain.analytics.snowflake_client import run_query

opty_id_15 = "0063y00001ANfq2"

rows = run_query(
    """
    SELECT COLUMN_NAME
    FROM SSE_DM_CSG_RPT_PRD.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'RENEWALS'
    AND TABLE_NAME = 'WV_CI_RENEWAL_OPTY_VW'
    AND (
        COLUMN_NAME LIKE '%%GMV%%'
        OR COLUMN_NAME LIKE '%%UTIL%%'
        OR COLUMN_NAME LIKE '%%BURN%%'
        OR COLUMN_NAME LIKE '%%RATE%%'
    )
    ORDER BY ORDINAL_POSITION
"""
)
print("GMV/UTIL/RATE columns in WV_CI_RENEWAL_OPTY_VW:")
for r in rows:
    print(f"  - {list(r.values())[0]}")

print()
print("Sample data for Adidas opp:")
rows = run_query(
    """
    SELECT RENEWAL_OPTY_ID, RENEWAL_AMT_CONV, RENEWAL_ATR_CONV,
           RENEWAL_FCAST_ATTRITION_CONV
    FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW
    WHERE RENEWAL_OPTY_ID = %s
    LIMIT 1
""",
    [opty_id_15],
)
if rows:
    for k, v in rows[0].items():
        print(f"  {k}: {v}")
