from domain.analytics.snowflake_client import run_query

account_id_15 = "00130000002xFEI"  # Adidas AG

print("=" * 60)
print("TESTING AV USAGE VIEWS - COLUMN DISCOVERY")
print("=" * 60)

# Check WV_AV_USAGE_EXTRACT_VW columns
print("\n1. WV_AV_USAGE_EXTRACT_VW columns:")
rows = run_query(
    """
    SELECT COLUMN_NAME
    FROM SSE_DM_CSG_RPT_PRD.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'CIDM'
    AND TABLE_NAME = 'WV_AV_USAGE_EXTRACT_VW'
    ORDER BY ORDINAL_POSITION
"""
)
for r in rows:
    print(f"  - {list(r.values())[0]}")

# Check WV_EWS360_SKINNY_USAGE_FINAL_VW columns
print("\n2. WV_EWS360_SKINNY_USAGE_FINAL_VW columns:")
rows = run_query(
    """
    SELECT COLUMN_NAME
    FROM SSE_DM_CSG_RPT_PRD.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'CIDM'
    AND TABLE_NAME = 'WV_EWS360_SKINNY_USAGE_FINAL_VW'
    ORDER BY ORDINAL_POSITION
"""
)
for r in rows:
    print(f"  - {list(r.values())[0]}")

# Check WV_AV_AOV_EXTRACT_VW columns
print("\n3. WV_AV_AOV_EXTRACT_VW columns:")
rows = run_query(
    """
    SELECT COLUMN_NAME
    FROM SSE_DM_CSG_RPT_PRD.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'CIDM'
    AND TABLE_NAME = 'WV_AV_AOV_EXTRACT_VW'
    ORDER BY ORDINAL_POSITION
"""
)
for r in rows:
    print(f"  - {list(r.values())[0]}")

# Check WV_AV_USAGE_DETAILS_VW columns
print("\n4. WV_AV_USAGE_DETAILS_VW columns:")
rows = run_query(
    """
    SELECT COLUMN_NAME
    FROM SSE_DM_CSG_RPT_PRD.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'CIDM'
    AND TABLE_NAME = 'WV_AV_USAGE_DETAILS_VW'
    ORDER BY ORDINAL_POSITION
"""
)
for r in rows:
    print(f"  - {list(r.values())[0]}")

print("\n" + "=" * 60)
print("NOW TESTING DATA FOR ADIDAS AG")
print("=" * 60)

# Test WV_AV_USAGE_EXTRACT_VW with actual data
print("\n5. WV_AV_USAGE_EXTRACT_VW - Adidas AG data:")
try:
    rows = run_query(
        """
        SELECT *
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
        WHERE ACCOUNT_ID = %s
        LIMIT 5
    """,
        [account_id_15],
    )
    if rows:
        print(f"  ✓ Found {len(rows)} rows!")
        for r in rows[:2]:
            for k, v in r.items():
                if v is not None:
                    print(f"    {k}: {v}")
            print()
    else:
        print("  ⚠️ No rows found")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:150]}")

# Test WV_EWS360_SKINNY_USAGE_FINAL_VW
print("\n6. WV_EWS360_SKINNY_USAGE_FINAL_VW - Adidas AG data:")
try:
    rows = run_query(
        """
        SELECT *
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_EWS360_SKINNY_USAGE_FINAL_VW
        WHERE ACCOUNT_ID = %s
        LIMIT 5
    """,
        [account_id_15],
    )
    if rows:
        print(f"  ✓ Found {len(rows)} rows!")
        for r in rows[:2]:
            for k, v in r.items():
                if v is not None:
                    print(f"    {k}: {v}")
            print()
    else:
        print("  ⚠️ No rows found")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:150]}")

print("\n" + "=" * 60)
