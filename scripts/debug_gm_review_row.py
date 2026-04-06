from domain.salesforce.org62_client import (
    get_red_account,
    get_renewal_opportunities,
    get_renewal_opportunities_any_cloud,
    resolve_account,
)
from domain.analytics.snowflake_client import (
    enrich_account,
    format_enrichment_for_display,
    fmt_amount,
    get_account_attrition,
    to_15_char_id,
)

print("=" * 70)
print("GM REVIEW ROW DATA - Adidas AG")
print("=" * 70)

acct = resolve_account("Adidas AG")
account_id = acct["id"]
account_id_15 = to_15_char_id(account_id)
account_name = " ".join(acct["name"].split())
print(f"\n✓ ACCOUNT: {account_name}")
print(f"  SF URL: https://org62.my.salesforce.com/{account_id}")

opps = get_renewal_opportunities(account_id, "Commerce Cloud")
if not opps:
    opps = get_renewal_opportunities_any_cloud(account_id)
opp = opps[0] if opps else {}
opp_id = opp.get("Id", "") if opp else ""
print(f'\n✓ OPP: {opp.get("Name", "N/A")}')

enrichment = enrich_account(account_id, opp_id, "Commerce Cloud")
display = format_enrichment_for_display(enrichment)
products = get_account_attrition(account_id_15, "Commerce Cloud")
all_products = get_account_attrition(account_id_15, None)
red = get_red_account(account_id)

print("\n" + "=" * 70)
print("COLUMN VALUES")
print("=" * 70)

ari_cat = display.get("ari_category", "Unknown")
ari_prob = display.get("ari_probability", "N/A")
print(f"\n1. ARI:              {ari_cat} ({ari_prob})")

renewal_aov = float(enrichment.get("renewal_aov", {}).get("renewal_aov", 0) or 0)
print(f"2. CC AOV:           ${renewal_aov:,.0f}  →  {fmt_amount(renewal_aov)}")

_ren = enrichment.get("renewal_aov", {}) or {}
renewal_atr = float(_ren.get("renewal_atr_snow", 0) or _ren.get("renewal_atr", 0) or 0)
print(f"3. ATR (Snow FCAST): ${renewal_atr:,.0f}  →  {fmt_amount(renewal_atr)}")

forecasted_atr = (
    abs(float(opp.get("Forecasted_Attrition__c", 0) or 0)) if opp else 0
)
print(f"4. For. Attrition:   ${forecasted_atr:,.0f}  →  {fmt_amount(forecasted_atr)}")

gmv_rate = display.get("gmv_rate", "Unknown")
print(f"5. GMV Rate:         {gmv_rate}")

util_rate = display.get("utilization_rate", "N/A")
util_emoji = display.get("util_emoji", "")
print(f"6. Util Rate:        {util_emoji} {util_rate}")

close_date = opp.get("CloseDate", "N/A") if opp else "N/A"
print(f"7. Close Date:       {close_date}")

csg_geo = enrichment.get("renewal_aov", {}).get("csg_geo", "N/A")
print(f"8. Territory:        {csg_geo}")

if all_products:
    unique_l1s = list(
        dict.fromkeys(
            str(p.get("APM_LVL_1", "") or "").strip()
            for p in all_products
            if p.get("APM_LVL_1")
        )
    )
    sf_products = ", ".join(unique_l1s) if unique_l1s else "N/A"
else:
    sf_products = "N/A"
print(f"9. SF Products:      {sf_products}")

specialist = (opp.get("Specialist_Sales_Notes__c") or "")[:100] if opp else ""
description = (opp.get("Description") or "")[:100] if opp else ""
notes = specialist or description or "N/A"
print(f"10. Notes:           {notes}")

if red:
    red_stage = red.get("Stage__c", "")
    days_red = red.get("Days_Red__c", "N/A")
    latest = (red.get("Latest_Updates__c") or "")[:100]
    latest = (
        latest.replace("<p>", "")
        .replace("</p>", " ")
        .replace("<br>", " ")
        .strip()
    )
    print(f"11. Red Account:     {red_stage} ({days_red} days)")
    print(f"    Latest Update:   {latest}")
else:
    print("11. Red Account:     N/A")

print("12. Risk Analysis:   [Will be AI-generated]")
print("13. Recommendation:  [Will be AI-generated]")

print("\n" + "=" * 70)
print("ISSUES TO FIX:")
print("=" * 70)

issues = []
if ari_cat == "Unknown":
    issues.append("❌ ARI: Unknown")
if renewal_aov == 0:
    issues.append("❌ CC AOV: $0 - renewal view not returning data")
if renewal_atr == 0:
    issues.append("❌ ATR: $0 - renewal ATR not populated")
if forecasted_atr == 0:
    issues.append("❌ For. Attrition: $0 - check Salesforce opp")
if gmv_rate == "Unknown":
    issues.append("⚠️  GMV Rate: Unknown - not in renewal view")
if util_rate == "N/A":
    issues.append("❌ Util Rate: N/A - CIDM query failing")
if csg_geo == "N/A":
    issues.append("⚠️  Territory: N/A - CSG_GEO not in enrichment")
if sf_products == "N/A":
    issues.append("❌ SF Products: N/A - no all-products attrition data")
if not notes or notes == "N/A":
    issues.append("⚠️  Notes: Empty")

if issues:
    for i in issues:
        print(i)
else:
    print("✅ All columns populated!")

print("=" * 70)
