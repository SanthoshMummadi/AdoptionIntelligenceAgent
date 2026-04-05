from domain.salesforce.org62_client import get_sf_client, _escape

sf = get_sf_client()
account_id = "00130000002xFEIAA2"

# Test 1: Simplest possible query
print("Test 1: Simple query...")
try:
    result = sf.query(
        f"SELECT Id, Name FROM Opportunity WHERE AccountId = '{account_id}' LIMIT 5"
    )
    print(f"  ✓ Found {len(result.get('records', []))} opportunities")
    for r in result.get("records", []):
        print(f"    - {r['Name']}")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:200]}")

print()

# Test 2: With Renewal filter
print("Test 2: With Renewal filter...")
try:
    result = sf.query(
        f"SELECT Id, Name, StageName FROM Opportunity WHERE AccountId = '{account_id}' AND Name LIKE '%Renewal%' LIMIT 5"
    )
    print(f"  ✓ Found {len(result.get('records', []))} renewals")
    for r in result.get("records", []):
        print(f"    - {r['Name']} ({r['StageName']})")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:200]}")

print()

# Test 3: Open opps only (IsClosed = false; NOT LIKE '%Closed%' breaks REST GET encoding)
print("Test 3: With IsClosed = false...")
try:
    result = sf.query(
        f"SELECT Id, Name, StageName FROM Opportunity WHERE AccountId = '{account_id}' "
        f"AND Name LIKE '%Renewal%' AND IsClosed = false LIMIT 5"
    )
    print(f"  ✓ Found {len(result.get('records', []))} open renewals (IsClosed = false)")
    for r in result.get("records", []):
        print(f"    - {r['Name']} ({r['StageName']})")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:200]}")

print()

# Test 4: Add full fields
print("Test 4: Full fields query...")
try:
    fields = "Id, Name, StageName, Amount, CloseDate, Forecasted_Attrition__c"
    query = f"SELECT {fields} FROM Opportunity WHERE AccountId = '{account_id}' AND Name LIKE '%Renewal%' LIMIT 5"
    result = sf.query(query)
    print(f"  ✓ Found {len(result.get('records', []))} renewals")
    for r in result.get("records", []):
        print(
            f"    - {r['Name']} | ATR: ${abs(r.get('Forecasted_Attrition__c') or 0):,.0f}"
        )
except Exception as e:
    print(f"  ❌ Error: {str(e)[:200]}")

print()

# Test 5: Print actual query from get_renewal_opportunities
print("Test 5: Print actual query being built...")
cloud = "Commerce Cloud"
cloud_escaped = _escape(cloud)
fields = (
    "Id, Name, StageName, Amount, CloseDate, "
    "Account.Id, Account.Name, Account.BillingCountry, "
    "ForecastCategoryName, Forecasted_Attrition__c, Swing__c, "
    "License_At_Risk_Reason__c, ACV_Reason_Detail__c, NextStep, "
    "Description, Specialist_Sales_Notes__c, "
    "Manager_Forecast_Judgement__c"
)
aid = _escape(account_id)
where = (
    f"AccountId = '{aid}' "
    f"AND Name LIKE '%{cloud_escaped}%' "
    f"AND Name LIKE '%Renewal%' "
    f"AND IsClosed = false"
)
query = f"SELECT {fields} FROM Opportunity WHERE {where} ORDER BY Forecasted_Attrition__c DESC NULLS LAST LIMIT 10"
print(f"  Query length: {len(query)}")
print(f"  First 300 chars: {query[:300]}")
print()
try:
    result = sf.query(query)
    print(f"  ✓ Found {len(result.get('records', []))} records")
except Exception as e:
    print(f"  ❌ Error: {str(e)[:300]}")
