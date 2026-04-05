import traceback

from domain.integrations.gsheet_exporter import GSHEET_ID, export_to_gsheet, get_google_creds

print(f"GSHEET_ID: {GSHEET_ID}")

try:
    creds = get_google_creds()
    print(f"✓ Creds: {type(creds).__name__}")
except Exception:
    print("❌ Creds error:")
    traceback.print_exc()

try:
    url = export_to_gsheet([], sheet_name="TEST")
    print(f"✓ Export: {url}")
except Exception:
    print("❌ Export error:")
    traceback.print_exc()
