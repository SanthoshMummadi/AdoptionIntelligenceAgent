from domain.salesforce.org62_client import (
    resolve_account,
    get_renewal_opportunities,
    get_renewal_opportunities_any_cloud,
    get_red_account,
)
from domain.analytics.snowflake_client import (
    enrich_account,
    format_enrichment_for_display,
    get_account_attrition,
    to_15_char_id,
    fmt_amount,
)
from domain.content.canvas_builder import clean_html

APM_L1_DISPLAY_MAP = {
    "Salesforce Platform": "Platform",
    "Integration": "MuleSoft",
    "AI and Data": "Data Cloud",
    "Cross Cloud - CRM": "CRM",
    "Cross Cloud - Einstein": "Einstein",
}
APM_L1_EXCLUDE = {"Other", ""}


def get_sf_products_display(all_products):
    unique_l1s = list(
        dict.fromkeys(
            str(p.get("APM_LVL_1") or "").strip()
            for p in all_products
            if str(p.get("APM_LVL_1") or "").strip()
        )
    )
    cleaned = []
    for l1 in unique_l1s:
        if l1 in APM_L1_EXCLUDE:
            continue
        display = APM_L1_DISPLAY_MAP.get(l1, l1)
        if display not in cleaned:
            cleaned.append(display)
    return ", ".join(cleaned) if cleaned else "N/A"


def get_gm_row(account_search: str):
    print(f"\n{'=' * 70}")
    print(f"GM ROW: {account_search}")
    print(f"{'=' * 70}")

    acct = resolve_account(account_search)
    if not acct:
        print("❌ Account not found")
        return

    account_id = acct["id"]
    account_id_15 = to_15_char_id(account_id)
    account_name = " ".join(acct["name"].split())
    sf_url = f"https://org62.my.salesforce.com/{account_id}"

    opps = get_renewal_opportunities(account_id, "Commerce Cloud")
    if not opps:
        opps = get_renewal_opportunities_any_cloud(account_id)
    opp = opps[0] if opps else {}
    opp_id = opp.get("Id", "") if opp else ""

    enrichment = enrich_account(account_id, opp_id, "Commerce Cloud")
    display = format_enrichment_for_display(enrichment)
    red = get_red_account(account_id)

    all_products = get_account_attrition(account_id_15, cloud=None)

    print(f"ACCOUNT:         [{account_name}]({sf_url})")

    ari_cat = display.get("ari_category", "Unknown")
    ari_prob = display.get("ari_probability", "N/A")
    print(f"ARI:             {ari_cat} ({ari_prob})")

    renewal_aov = float(enrichment.get("renewal_aov", {}).get("renewal_aov", 0) or 0)
    print(f"CC AOV:          ${renewal_aov:,.0f}  →  {fmt_amount(renewal_aov)}")

    ren = enrichment.get("renewal_aov", {}) or {}
    renewal_atr = float(
        ren.get("renewal_atr_snow", 0) or ren.get("renewal_atr", 0) or 0
    )
    print(f"ATR (Snow FCAST): {fmt_amount(renewal_atr)}")

    forecasted_atr = (
        abs(float(opp.get("Forecasted_Attrition__c", 0) or 0)) if opp else 0
    )
    print(f"For. Attrition:  $-{forecasted_atr:,.0f}  →  {fmt_amount(forecasted_atr)}")

    gmv_rate = display.get("gmv_rate", "N/A")
    print(f"GMV Rate:        {gmv_rate}")

    util_rate = display.get("utilization_rate", "N/A")
    util_emoji = display.get("util_emoji", "")
    print(f"Util Rate:       {util_emoji} {util_rate}")

    close_date = opp.get("CloseDate", "N/A") if opp else "N/A"
    print(f"Close Date:      {close_date}")

    territory = enrichment.get("renewal_aov", {}).get("csg_geo", "N/A") or "N/A"
    print(f"Territory:       {territory}")

    sf_products = get_sf_products_display(all_products)
    print(f"SF Products:     {sf_products}")

    specialist = (opp.get("Specialist_Sales_Notes__c") or "")[:150] if opp else ""
    description = (opp.get("Description") or "")[:150] if opp else ""
    notes = specialist or description or "N/A"
    notes = notes.replace("|", "-")
    print(f"Notes:           {notes[:80]}...")

    if red:
        red_stage = red.get("Stage__c", "")
        days_red = red.get("Days_Red__c", "N/A")
        latest = clean_html((red.get("Latest_Updates__c") or "")[:100])
        print(f"Red Account:     {red_stage} ({days_red} days)")
        print(f"Latest Update:   {latest[:80]}...")
    else:
        print("Red Account:     N/A")

    print("\n✅ Row complete - 11 data columns populated!")


if __name__ == "__main__":
    get_gm_row("Adidas AG")
    get_gm_row("Oxford Industries")
