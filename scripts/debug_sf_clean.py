from domain.salesforce.org62_client import resolve_account
from domain.analytics.snowflake_client import get_account_attrition, to_15_char_id

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


for name in ["Adidas AG", "GNC Holdings"]:
    acct = resolve_account(name)
    if acct:
        prods = get_account_attrition(to_15_char_id(acct["id"]), cloud=None)
        print(f"{name}: {get_sf_products_display(prods)}")
    else:
        print(f"{name}: (not resolved)")
