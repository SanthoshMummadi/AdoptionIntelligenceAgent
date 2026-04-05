import os

from dotenv import load_dotenv

load_dotenv()

from domain.salesforce.org62_client import (
    get_renewal_opportunities,
    get_renewal_opportunities_any_cloud,
    resolve_account,
    resolve_account_enhanced,
)

print("=" * 60)
print("TESTING ADIDAS AG - FULL WORKFLOW PATH")
print("=" * 60)

# Test 1: Domain resolve
print("\n1. Domain resolve_account:")
acct = resolve_account("Adidas AG")
print(f'  ✓ {acct["name"]} ({acct["id"]})')
account_id = acct["id"]

# Test 2: Domain get_renewal_opportunities
print("\n2. Domain get_renewal_opportunities:")
opps = get_renewal_opportunities(account_id, "Commerce Cloud")
print(f"  Found: {len(opps)} opps")
if opps:
    print(f'  ✓ {opps[0].get("Name")} | {opps[0].get("CloseDate")}')

# Test 3: Same as adapter path — cloud renewal then any-cloud fallback
print("\n3. Renewal pick (cloud then any-cloud):")
op2 = get_renewal_opportunities(account_id, "Commerce Cloud")
if not op2:
    op2 = get_renewal_opportunities_any_cloud(account_id)
opp = op2[0] if op2 else {}
if opp:
    print(f'  ✓ {opp.get("Name")} | {opp.get("CloseDate")}')
else:
    print("  ❌ No renewal opp!")

# Test 4: resolve_account_enhanced (workflow name resolution)
print("\n4. resolve_account_enhanced:")
try:
    result = resolve_account_enhanced("Adidas AG", cloud="Commerce Cloud")
    if result:
        print(f'  ✓ {result.get("name")} ({result.get("id")})')
    else:
        print("  ❌ Returned empty!")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:100]}")

_any = get_renewal_opportunities_any_cloud(account_id)
print(f"\n  (any cloud renewal opps: {len(_any)})")

print("\n" + "=" * 60)
