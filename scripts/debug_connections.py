print("Starting connection test...")

# Import multiple times — should only log ONCE
from domain.analytics.snowflake_client import get_snowflake_connection
from domain.analytics.snowflake_client import run_query

print("Testing multiple connections...")
conn1 = get_snowflake_connection()
conn2 = get_snowflake_connection()
conn3 = get_snowflake_connection()

print(f"conn1 is conn2: {conn1 is conn2}")  # Should be True
print(f"conn1 is conn3: {conn1 is conn3}")  # Should be True
print()

# Run a few queries
r1 = run_query("SELECT 1 as test")
r2 = run_query("SELECT 2 as test")
r3 = run_query("SELECT 3 as test")

print(f"Queries ran: {r1}, {r2}, {r3}")
print()
print('✅ If you see only ONE "Connected to Snowflake" above - fix is working!')
print("❌ If you see multiple — connection singleton is broken")
