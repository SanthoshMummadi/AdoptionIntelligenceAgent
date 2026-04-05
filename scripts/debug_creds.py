import json
import os
from pathlib import Path

from dotenv import load_dotenv

_REPO = Path(__file__).resolve().parent
load_dotenv(_REPO / ".env")
load_dotenv()

print("=" * 60)
print("ALL CREDENTIAL SOURCES")
print("=" * 60)

print("\n1. ENV Variables:")
for key in ("GOOGLE_SERVICE_ACCOUNT_JSON", "GSPREAD_CREDENTIALS"):
    val = os.getenv(key)
    if val:
        if val.strip().startswith("{"):
            try:
                info = json.loads(val)
                print(f'  {key}: client_email = {info.get("client_email")}')
            except Exception:
                print(f"  {key}: SET but not valid JSON")
        else:
            print(f"  {key}: SET but not JSON body (length: {len(val)})")
    else:
        print(f"  {key}: NOT SET")

print("\n2. Credential Files:")
search_paths = [
    _REPO,
    _REPO / "domain",
    _REPO / "domain" / "integrations",
]
for path in search_paths:
    for fname in ("credentials.json", "google_creds.json", "service_account.json"):
        fpath = path / fname
        if fpath.is_file():
            with open(fpath) as f:
                info = json.load(f)
            print(f"  {fpath}:")
            print(f'    client_email: {info.get("client_email")}')

print("\n3. What get_google_creds() returns:")
from domain.integrations.gsheet_exporter import get_google_creds

creds = get_google_creds()
print(f"  service_account_email: {creds.service_account_email}")
print("=" * 60)
