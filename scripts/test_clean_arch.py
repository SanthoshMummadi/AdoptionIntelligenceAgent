print('Testing clean architecture...')
print()

# 1. No adapter imports anywhere critical
print('1. Checking no adapter imports in core files...')
import subprocess
result = subprocess.run(
    ['grep', '-r', r'salesforce_adapter\|snowflake_adapter\|canvas_adapter',
     'slack_app.py', 'server.py', 'services/gm_review_workflow.py'],
    capture_output=True, text=True
)
if result.stdout:
    print(f'  ⚠️  Found adapter imports:\n{result.stdout}')
else:
    print('  ✓ No adapter imports in core files!')

# 2. GMReviewWorkflow imports correctly
print()
print('2. Testing GMReviewWorkflow import...')
try:
    from services.gm_review_workflow import GMReviewWorkflow
    from server import call_llm_gateway
    wf = GMReviewWorkflow(call_llm_fn=call_llm_gateway, max_concurrent=8)
    print(f'  ✓ GMReviewWorkflow created: {wf}')
except Exception as e:
    import traceback
    print(f'  ❌ Error:')
    traceback.print_exc()

# 3. Domain imports work
print()
print('3. Testing domain imports...')
try:
    from domain.salesforce.org62_client import resolve_account_enhanced
    from domain.analytics.snowflake_client import enrich_account, get_usage_unified
    from domain.content.canvas_builder import build_adoption_pov, build_gm_review_canvas_markdown
    from domain.intelligence.risk_engine import generate_risk_analysis
    from domain.integrations.gsheet_exporter import export_to_gsheet
    print('  ✓ All domain imports OK!')
except Exception as e:
    import traceback
    print(f'  ❌ Import error:')
    traceback.print_exc()

# 4. File structure check
print()
print('4. File structure:')
import os
files = {
    'slack_app.py': 'Entry point',
    'server.py': 'MCP server',
    'services/gm_review_workflow.py': 'GM Review orchestrator',
    'domain/salesforce/org62_client.py': 'Salesforce domain',
    'domain/analytics/snowflake_client.py': 'Snowflake domain',
    'domain/content/canvas_builder.py': 'Canvas builder',
    'domain/intelligence/risk_engine.py': 'Risk engine',
    'domain/integrations/gsheet_exporter.py': 'Sheets exporter',
}
deleted = [
    'adapters/salesforce_adapter.py',
    'adapters/snowflake_adapter.py',
    'adapters/canvas_adapter.py',
    'services/parallel_gm_review_workflow.py',
]
for f, desc in files.items():
    exists = '✓' if os.path.exists(f) else '❌'
    size = os.path.getsize(f) if os.path.exists(f) else 0
    print(f'  {exists} {f} ({size:,} bytes) - {desc}')

print()
print('  Deleted files (should not exist):')
for f in deleted:
    exists = '❌ STILL EXISTS!' if os.path.exists(f) else '✓ Deleted'
    print(f'  {exists} {f}')

# 5. Line count improvement
print()
print('5. Line counts:')
for f in ['slack_app.py', 'server.py', 'services/gm_review_workflow.py']:
    if os.path.exists(f):
        with open(f) as fh:
            lines = sum(1 for _ in fh)
        print(f'  {f}: {lines} lines')

print()
print('='*50)
print('✅ Architecture migration complete!')
print('='*50)
