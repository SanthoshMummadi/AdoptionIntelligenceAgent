from domain.analytics.snowflake_client import run_query

account_id_15 = "00130000002xFEI"  # Adidas AG — reserved for follow-up row-level tests

print("=" * 60)
print("CHECKING SSE_DM_CSG_RPT_PRD AV USAGE TABLES")
print("=" * 60)

# Step 1: Find all AV usage related tables
print("\n1. Finding AV Usage tables...")
rows = run_query(
    """
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM SSE_DM_CSG_RPT_PRD.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_NAME LIKE '%AV%'
    OR TABLE_NAME LIKE '%USAGE%'
    OR TABLE_NAME LIKE '%UTIL%'
    ORDER BY TABLE_SCHEMA, TABLE_NAME
"""
)
for r in rows:
    print(f"  {r.get('TABLE_SCHEMA')}.{r.get('TABLE_NAME')}")

# Step 2: Check CIDM schema tables
print("\n2. Finding CIDM tables...")
rows = run_query(
    """
    SELECT TABLE_SCHEMA, TABLE_NAME
    FROM SSE_DM_CSG_RPT_PRD.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'CIDM'
    ORDER BY TABLE_NAME
"""
)
for r in rows:
    print(f"  {r.get('TABLE_SCHEMA')}.{r.get('TABLE_NAME')}")
