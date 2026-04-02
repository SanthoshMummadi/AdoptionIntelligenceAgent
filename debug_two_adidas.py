import os

from dotenv import load_dotenv

load_dotenv()

from domain.salesforce.org62_client import get_renewal_opportunities, get_sf_client

sf = get_sf_client()

id1 = "00130000002xFEIAA2"  # From resolve_account (correct)
id2 = "0010M00001QkZKIQA3"  # From adapter (wrong)

for acct_id in [id1, id2]:
    print(f"\nAccount ID: {acct_id}")

    result = sf.query(
        f"SELECT Id, Name, Type, IsPersonAccount FROM Account WHERE Id = '{acct_id}' LIMIT 1"
    )
    if result.get("records"):
        r = result["records"][0]
        print(f'  Name: {r.get("Name")}')
        print(f'  Type: {r.get("Type")}')

    opps = get_renewal_opportunities(acct_id, "Commerce Cloud")
    print(f"  Renewal opps: {len(opps)}")
    if opps:
        print(f'  First opp: {opps[0].get("Name")}')
