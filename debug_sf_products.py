from domain.salesforce.org62_client import resolve_account
from domain.analytics.snowflake_client import get_account_attrition, to_15_char_id

acct = resolve_account("Adidas AG")
account_id_15 = to_15_char_id(acct["id"])

print("ALL products (no cloud filter):")
all_products = get_account_attrition(account_id_15, cloud=None)
print(f"Found {len(all_products)} total products")

unique_l1s = list(
    dict.fromkeys(
        str(p.get("APM_LVL_1") or "").strip()
        for p in all_products
        if p.get("APM_LVL_1")
    )
)
print(f"\nUnique APM L1 ({len(unique_l1s)}):")
for l1 in unique_l1s:
    print(f"  - {l1}")

print(f'\nSF Products cell: {", ".join(unique_l1s)}')

print("\n" + "=" * 50)
print("GNC Holdings - ALL products:")
gnc = resolve_account("GNC Holdings")
if gnc:
    gnc_id_15 = to_15_char_id(gnc["id"])
    gnc_products = get_account_attrition(gnc_id_15, cloud=None)

    unique_l1s_gnc = list(
        dict.fromkeys(
            str(p.get("APM_LVL_1") or "").strip()
            for p in gnc_products
            if p.get("APM_LVL_1")
        )
    )
    print(f"Unique APM L1: {', '.join(unique_l1s_gnc)}")
    print("Expected: Platform, Analytics, Marketing, Sales, Service")
