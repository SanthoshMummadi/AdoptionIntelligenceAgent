from domain.analytics.snowflake_client import get_usage_raw_data, to_15_char_id
from domain.content.canvas_builder import build_adoption_pov
from domain.salesforce.org62_client import resolve_account

for name in ['Adidas AG', 'Oxford Industries']:
    acct = resolve_account(name)
    if not acct:
        print(f'❌ {name} not found')
        continue
    
    account_id_15 = to_15_char_id(acct['id'])
    print(f'\n{"="*50}')
    print(f'ADOPTION POV: {name}')
    print(f'{"="*50}')
    
    usage_raw = get_usage_raw_data(account_id_15, cloud='Commerce Cloud')
    print(f'Raw usage rows: {len(usage_raw)}')
    
    pov = build_adoption_pov(usage_raw, cloud='Commerce Cloud')
    print(f'Adoption POV:\n{pov}')
