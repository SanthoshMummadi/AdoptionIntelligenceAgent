import traceback

import gspread

from domain.integrations.gsheet_exporter import GSHEET_ID, get_google_creds


def _explain_access():
    try:
        c = get_google_creds()
        email = getattr(c, "service_account_email", None) or "(run get_sa_email.py)"
        print(
            "\n  → Fix: In Google Sheets, Share this spreadsheet with the service account:\n"
            f"     {email}\n"
            "     Role: Editor. Also ensure the file isn’t blocked by Workspace “Restrict access”.\n"
        )
    except Exception:
        pass


print(f"GSHEET_ID: {GSHEET_ID}")

print("\n1. Testing gspread connection...")
try:
    creds = get_google_creds()
    gc = gspread.authorize(creds)
    print("✓ gspread authorized")

    sh = gc.open_by_key(GSHEET_ID)
    print(f"✓ Sheet opened: {sh.title}")

    worksheets = sh.worksheets()
    print(f"✓ Worksheets: {[ws.title for ws in worksheets]}")

except PermissionError:
    print("❌ Permission denied opening spreadsheet (403).")
    _explain_access()
    traceback.print_exc()
except gspread.exceptions.APIError as e:
    if "403" in str(e):
        print("❌ API 403 — caller does not have permission.")
        _explain_access()
    else:
        print(f"❌ API error: {e}")
    traceback.print_exc()
except Exception:
    print("❌ Error:")
    traceback.print_exc()

print("\n2. Testing write...")
try:
    creds = get_google_creds()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(GSHEET_ID)

    try:
        ws = sh.worksheet("TEST")
    except Exception:
        ws = sh.add_worksheet(title="TEST", rows=10, cols=25)

    ws.append_row(["Test", "Row", "Write"], value_input_option="USER_ENTERED")
    print("✓ Write successful!")

except PermissionError:
    print("❌ Write blocked — same 403 / sharing issue.")
    _explain_access()
    traceback.print_exc()
except gspread.exceptions.APIError as e:
    if "403" in str(e):
        print("❌ API 403 on write.")
        _explain_access()
    else:
        print(f"❌ API error: {e}")
    traceback.print_exc()
except Exception:
    print("❌ Write error:")
    traceback.print_exc()
