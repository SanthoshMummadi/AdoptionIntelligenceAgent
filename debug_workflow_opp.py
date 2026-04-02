from dotenv import load_dotenv

load_dotenv()

from domain.salesforce.org62_client import (
    get_renewal_opportunities,
    get_renewal_opportunities_any_cloud,
)

# Test get_renewal flow (no adapter — matches GMReviewWorkflow)
print("Testing get_renewal_opportunities / any_cloud...")
account_id = "00130000002xFEIAA2"

try:
    opp_list = get_renewal_opportunities(account_id, "Commerce Cloud")
    if not opp_list:
        opp_list = get_renewal_opportunities_any_cloud(account_id)
    opp = opp_list[0] if opp_list else None
    if opp:
        print(f'✓ Found: {opp.get("Name")}')
        print(f'  ID: {opp.get("Id")}')
        print(f'  Close: {opp.get("CloseDate")}')
    else:
        print("❌ No opp found")
        opps = get_renewal_opportunities(account_id, "Commerce Cloud")
        print(f"Domain function found: {len(opps)} opps")
        if opps:
            print(f'  First: {opps[0].get("Name")}')
except Exception:
    import traceback

    traceback.print_exc()
