from domain.salesforce.org62_client import get_sf_client
from domain.analytics.snowflake_client import (
    enrich_account,
    format_enrichment_for_display,
    get_account_attrition,
)

print("=" * 70)
print("FULL SYSTEM TEST - Opportunity 0063y00001ANfq2AAD")
print("=" * 70)

# Step 1: Get opportunity details from Salesforce
sf = get_sf_client()
result = sf.query("""
    SELECT
        Id, Name, StageName, Amount, CloseDate,
        Account.Id, Account.Name, Account.BillingCountry,
        ForecastCategoryName,
        Forecasted_Attrition__c, Swing__c,
        License_At_Risk_Reason__c,
        ACV_Reason_Detail__c, NextStep,
        Description, Specialist_Sales_Notes__c,
        Manager_Forecast_Judgement__c
    FROM Opportunity
    WHERE Id = '0063y00001ANfq2AAD'
    LIMIT 1
""")

if not result.get("records"):
    print("Opportunity not found")
else:
    opp = result["records"][0]
    acct_data = opp.get("Account") or {}
    account_id = acct_data.get("Id", "")
    account_name = acct_data.get("Name", "Unknown")

    print(f"Opportunity: {opp['Name']}")
    print(f"  Opp ID: {opp['Id']}")
    print(f"  Stage: {opp['StageName']}")
    print(f"  Close Date: {opp.get('CloseDate', 'N/A')}")
    print()
    print(f"Account: {account_name}")
    print(f"  Account ID: {account_id}")
    print(f"  Country: {acct_data.get('BillingCountry', 'N/A')}")

    forecasted_atr = abs(opp.get("Forecasted_Attrition__c", 0) or 0)
    print(f"  Forecasted Attrition: ${forecasted_atr:,.0f}")
    print(f"  Risk Reason: {opp.get('License_At_Risk_Reason__c', 'N/A')}")
    print()

    # Step 2: Snowflake enrichment
    print("Enriching with Snowflake data...")
    e = enrich_account(account_id, opp["Id"], "Commerce Cloud")
    d = format_enrichment_for_display(e)

    print()
    print("=" * 70)
    print(f"ENRICHMENT RESULTS - {account_name}")
    print("=" * 70)
    print(f"ARI Score:        {d.get('ari_emoji')} {d.get('ari_category')} ({d.get('ari_probability')})")
    print(f"ARI Reason:       {d.get('ari_reason', 'N/A')}")
    print(f"Health:           {d.get('health_display')}")
    print(f"Cloud AOV:        {d.get('cc_aov')}")
    print(f"Utilization:      {d.get('utilization_rate')}")
    print(f"Renewal ATR:      ${d.get('renewal_atr', 0):,.0f}")
    print("=" * 70)

    # Step 3: Product breakdown (normalized keys from get_account_attrition)
    print()
    print("Product-Level Attrition Breakdown:")
    product_attrition = get_account_attrition(account_id, "Commerce Cloud")
    for p in product_attrition[:5]:
        prod_name = p.get("product") or p.get("APM_LVL_3", "Unknown")
        prod_risk = p.get("category") or p.get("ATTRITION_PROBA_CATEGORY", "Unknown")
        raw_atr = p.get("attrition")
        if raw_atr is None:
            prod_atr = abs(float(p.get("ATTRITION_PIPELINE") or 0))
        else:
            prod_atr = abs(float(raw_atr))
        prod_reason = (p.get("reason") or p.get("ATTRITION_REASON") or "N/A")[:30]

        emoji = "🔴" if prod_risk == "High" else "🟡" if prod_risk == "Medium" else "🟢"
        print(f"  {emoji} {prod_name}: {prod_risk} (${prod_atr:,.0f}) - {prod_reason}")

    print()
    print("=" * 70)
    print("ALL SYSTEMS OPERATIONAL")
    print("=" * 70)
