import traceback

print("=" * 60)
print("ISSUE 1: Google Sheets Error")
print("=" * 60)
try:
    from domain.integrations.gsheet_exporter import GSHEET_ID, get_google_creds

    print(f'GSHEET_ID: {GSHEET_ID[:20] if GSHEET_ID else "NOT SET"}...')
    creds = get_google_creds()
    print(f"✓ Creds OK: {type(creds).__name__}")
except Exception:
    traceback.print_exc()

print()
print("=" * 60)
print("ISSUE 2: SOQL — scan org62_client")
print("=" * 60)
with open("domain/salesforce/org62_client.py", "r") as f:
    content = f.read()

lines = content.split("\n")
for i, line in enumerate(lines, 1):
    if "sf.query(" in line or "LIKE" in line or "SELECT" in line:
        print(f"  Line {i}: {line.strip()[:100]}")

print()
print("=" * 60)
print("ISSUE 3: Adidas AG - Why Unknown/NA values")
print("=" * 60)
from domain.salesforce.org62_client import (
    get_renewal_opportunities,
    get_renewal_opportunities_any_cloud,
    resolve_account,
)
from domain.analytics.snowflake_client import (
    enrich_account,
    format_enrichment_for_display,
    to_15_char_id,
)

acct = resolve_account("Adidas AG")
if not acct or not acct.get("id"):
    print("❌ resolve_account('Adidas AG') failed")
    raise SystemExit(1)
account_id = acct["id"]
account_id_15 = to_15_char_id(account_id)

print(f"Account ID: {account_id}")

# Check renewal opps
print("\nChecking renewal opportunities...")
opps = get_renewal_opportunities(account_id, "Commerce Cloud")
print(f"Cloud-specific opps: {len(opps)}")
if not opps:
    opps = get_renewal_opportunities_any_cloud(account_id)
    print(f"Any-cloud opps: {len(opps)}")

if opps:
    opp = opps[0]
    opp_id = opp.get("Id", "")
    print(f'✓ Opp found: {opp.get("Name")}')
    print(f"  Opp ID: {opp_id}")
    print(f'  Stage: {opp.get("StageName")}')
    print(f'  Close: {opp.get("CloseDate")}')
    print(
        f'  Forecasted ATR: ${abs(opp.get("Forecasted_Attrition__c", 0) or 0):,.0f}'
    )

    # Check enrichment
    print("\nRunning enrichment...")
    enrichment = enrich_account(account_id, opp_id, "Commerce Cloud")
    display = format_enrichment_for_display(enrichment)

    print("\nEnrichment results:")
    print(f'  ARI:       {display.get("ari_category")} ({display.get("ari_probability")})')
    print(f'  Health:    {display.get("health_display")}')
    print(f'  CC AOV:    {display.get("cc_aov")}')
    print(f'  Util:      {display.get("utilization_rate")}')
    print(f'  Renewal AOV: {enrichment.get("renewal_aov", {}).get("renewal_aov")}')
    print(f'  Renewal ATR: {enrichment.get("renewal_aov", {}).get("renewal_atr")}')
    print(f'  CSG GEO:   {enrichment.get("renewal_aov", {}).get("csg_geo")}')
else:
    print("❌ NO OPPS FOUND - This is why values are Unknown/NA!")
    print("The SOQL query is broken for this account in the workflow")

print()
print("=" * 60)
