"""
Trace where each field comes from for Adidas AG (Salesforce org62 + Snowflake).
"""
from domain.salesforce.org62_client import (
    get_renewal_opportunities,
    get_renewal_opportunities_any_cloud,
    get_red_account,
    resolve_account,
)
from domain.analytics.snowflake_client import (
    enrich_account,
    format_enrichment_for_display,
    get_account_attrition,
    get_ari_score,
    get_ari_score_by_account,
    get_customer_health,
    get_renewal_aov,
    get_usage_unified,
    to_15_char_id,
)

print("=" * 70)
print("DATA SOURCE TRACING - Adidas AG")
print("=" * 70)
print()

# Step 1: Resolve account name
print("STEP 1: Account Resolution (Salesforce SOSL/SOQL)")
print("-" * 70)
acct = resolve_account("Adidas AG", cloud="Commerce Cloud")
if acct:
    print("✓ SOURCE: Salesforce org62")
    print(f"  Account Name: {acct['name']}")
    print(f"  Account ID: {acct['id']}")
    print(f"  Billing Country: {acct.get('billing_country', 'N/A')}")
else:
    print("❌ Not found")
    raise SystemExit(1)

account_id = acct["id"]
account_id_15 = to_15_char_id(account_id)
print(f"  15-char ID: {account_id_15}")
print()

# Step 2: Get renewal opportunities
print("STEP 2: Renewal Opportunities (Salesforce)")
print("-" * 70)
opps = get_renewal_opportunities(account_id, "Commerce Cloud")
print("✓ SOURCE: Salesforce org62 query")
print(f"  Found {len(opps)} Commerce Cloud renewal(s)")

if not opps:
    print("  Trying any-cloud fallback...")
    opps = get_renewal_opportunities_any_cloud(account_id)
    print(f"  Found {len(opps)} renewal(s) (any cloud)")

opp = {}
opp_id = None
opp_id_15 = None
if opps:
    opp = opps[0]
    opp_id = opp["Id"]
    opp_id_15 = to_15_char_id(opp_id)
    print(f"  Opp Name: {opp['Name']}")
    print(f"  Opp ID (18): {opp_id}")
    print(f"  Opp ID (15): {opp_id_15}")
    print(f"  Stage: {opp['StageName']}")
    print(
        f"  Forecasted_Attrition__c: ${abs(opp.get('Forecasted_Attrition__c', 0) or 0):,.0f}"
    )
else:
    print("  ⚠️ No renewal opportunities found")

print()

# Step 3: Snowflake ARI (Opportunity-level)
print("STEP 3: ARI Score - Opportunity Level (Snowflake CSS)")
print("-" * 70)
ari_opp = None
if opp_id_15:
    ari_opp = get_ari_score(opp_id)
    if ari_opp:
        print("✓ SOURCE: CSS.ATTRITION_PREDICTION_OPPTY")
        print(f"  Query: WHERE RENEWAL_OPTY_ID = '{to_15_char_id(opp_id)}'")
        print(f"  ARI Category: {ari_opp.get('ATTRITION_PROBA_CATEGORY')}")
        print(f"  ARI Probability: {ari_opp.get('ATTRITION_PROBA')}")
        print(f"  ARI Reason: {ari_opp.get('ATTRITION_REASON')}")
    else:
        print("  ⚠️ No ARI data for this opportunity")
else:
    print("  ⚠️ No opportunity ID to query")

print()

# Step 4: Snowflake ARI (Account-level fallback)
print("STEP 4: ARI Score - Account Level Fallback (Snowflake CSS)")
print("-" * 70)
ari_acct = get_ari_score_by_account(account_id_15, "Commerce Cloud")
if ari_acct:
    print("✓ SOURCE: CSS.ATTRITION_PREDICTION_ACCT_PRODUCT")
    print(f"  Query: WHERE ACCOUNT_ID = '{account_id_15}' AND cloud filter")
    print(f"  Found {len(ari_acct)} product(s) with ARI")
    for idx, a in enumerate(ari_acct[:3], 1):
        print(f"  #{idx} Product: {a.get('APM_LVL_3', 'Unknown')}")
        print(
            f"      ARI: {a.get('ATTRITION_PROBA_CATEGORY')} ({a.get('ATTRITION_PROBA')})"
        )
else:
    print("  ⚠️ No account-level ARI data")

print()

# Step 5: Customer Health
print("STEP 5: Customer Health (Snowflake CSS)")
print("-" * 70)
health = get_customer_health(account_id_15)
if health:
    print("✓ SOURCE: CSS.CI_CH_FACT_CUSTOMER_HEALTH_VW")
    print(f"  Query: WHERE ACCOUNT_ID = '{account_id_15}'")
    print(f"  Overall Score: {health.get('overall_score')}")
    print(f"  Overall Literal: {health.get('overall_literal')}")
    print(f"  Categories: {len(health.get('categories', []))}")
else:
    print("  ⚠️ No health data")

print()

# Step 6: Renewal AOV
print("STEP 6: Renewal AOV (Snowflake RENEWALS)")
print("-" * 70)
renewal = {}
if opp_id_15:
    renewal = get_renewal_aov(opp_id) or {}
    if renewal:
        print("✓ SOURCE: RENEWALS.WV_CI_RENEWAL_OPTY_VW")
        print(f"  Query: WHERE RENEWAL_OPTY_ID = '{to_15_char_id(opp_id)}'")
        print(f"  Account Name: {renewal.get('account_name')}")
        print(f"  Target Cloud: {renewal.get('target_cloud')}")
        print(f"  Renewal AOV: ${renewal.get('renewal_aov', 0):,.0f}")
        print(f"  Renewal ATR: ${renewal.get('renewal_atr', 0):,.0f}")
    else:
        print("  ⚠️ No renewal data in RENEWALS view")
else:
    print("  ⚠️ No opportunity ID to query")

print()

# Step 7: Product Attrition Breakdown
print("STEP 7: Product Attrition (Snowflake CSS)")
print("-" * 70)
products = get_account_attrition(account_id_15, "Commerce Cloud")
if products:
    print("✓ SOURCE: CSS.ATTRITION_PREDICTION_ACCT_PRODUCT")
    print(f"  Query: WHERE ACCOUNT_ID = '{account_id_15}' AND cloud filter")
    print(f"  Found {len(products)} product(s)")
    for p in products[:5]:
        print(
            f"  - {p.get('APM_LVL_3', 'Unknown')}: "
            f"{p.get('ATTRITION_PROBA_CATEGORY')} "
            f"(${abs(p.get('attrition', p.get('ATTRITION_PIPELINE', 0)) or 0):,.0f})"
        )
else:
    print("  ⚠️ No product attrition data")

print()

# Step 8: Red Account
print("STEP 8: Red Account (Salesforce)")
print("-" * 70)
red = get_red_account(account_id)
if red:
    print("✓ SOURCE: Salesforce Red_Account__c")
    print(f"  Query: WHERE Red_Account__c = '{account_id}'")
    print(f"  Stage: {red.get('Stage__c')}")
    print(f"  Days Red: {red.get('Days_Red__c')}")
    print(f"  Latest Updates: {(red.get('Latest_Updates__c') or '')[:50]}...")
else:
    print("  ⚠️ No red account record")

print()

print("STEP 9: Usage/Utilization (CIDM.WV_AV_USAGE_EXTRACT_VW)")
print("-" * 70)
usage = get_usage_unified(account_id_15, "Commerce Cloud").get("summary") or {}
if usage:
    print("✓ SOURCE: CIDM.WV_AV_USAGE_EXTRACT_VW")
    print(f"  Utilization: {usage.get('utilization_rate')}")
    print(f"  Source: {usage.get('source')}")
    print(f"  GMV Util: {usage.get('gmv_util')}")
else:
    print("  ⚠️ No usage data found")

print()
print("=" * 70)
print("SUMMARY")
print("=" * 70)

# Now run full enrichment
enrichment = enrich_account(account_id, opp_id if opps else None, "Commerce Cloud")
display = format_enrichment_for_display(enrichment)

ari_source = "NOT FOUND"
if opp_id_15 and get_ari_score(opp_id):
    ari_source = "Opp-level CSS.ATTRITION_PREDICTION_OPPTY"
elif ari_acct:
    ari_source = "Account-level CSS.ATTRITION_PREDICTION_ACCT_PRODUCT"

print(f"ARI Category:     {display.get('ari_category')} - FROM: {ari_source}")
print(
    f"Health Score:     {display.get('health_score')} - FROM: CSS.CI_CH_FACT_CUSTOMER_HEALTH_VW"
)
print(
    f"Cloud AOV:        {display.get('cc_aov')} - FROM: "
    f"{('RENEWALS.WV_CI_RENEWAL_OPTY_VW' if renewal and renewal.get('renewal_aov') else 'NOT FOUND')}"
)
_util = display.get("utilization_rate")
_u = enrichment.get("usage") or {}
if _util not in (None, "", "N/A") and str(_util).strip():
    _util_src = (
        f"CIDM.WV_AV_USAGE_EXTRACT_VW ({_u.get('source')})"
        if _u.get("source")
        else "CIDM.WV_AV_USAGE_EXTRACT_VW"
    )
else:
    _util_src = "NOT FOUND"
print(f"Utilization:      {_util} - FROM: {_util_src}")
print(
    f"Forecasted ATR:   ${abs(opp.get('Forecasted_Attrition__c', 0) or 0):,.0f} - FROM: "
    f"Salesforce Opportunity.Forecasted_Attrition__c"
)
print()
print("=" * 70)
