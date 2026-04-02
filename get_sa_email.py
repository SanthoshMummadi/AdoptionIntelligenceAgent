import json
import os
from pathlib import Path

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent
load_dotenv(_REPO_ROOT / ".env")
load_dotenv()

creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or os.getenv("GSPREAD_CREDENTIALS")

if creds_json and creds_json.strip().startswith("{"):
    try:
        info = json.loads(creds_json)
        print("From ENV:")
        print(f'  client_email: {info.get("client_email")}')
        print(f'  project_id:   {info.get("project_id")}')
        print(f'  type:         {info.get("type")}')
    except Exception as e:
        print(f"ENV parse error: {e}")
elif creds_json:
    print(f"From ENV: value set but not JSON (length {len(creds_json)}); try credentials file below.\n")

for fname in ("credentials.json", "google_creds.json"):
    fpath = _REPO_ROOT / fname
    if fpath.is_file():
        with open(fpath) as f:
            info = json.load(f)
        print(f"\nFrom {fname}:")
        print(f'  client_email: {info.get("client_email")}')
        print(f'  project_id:   {info.get("project_id")}')
        print(f'  type:         {info.get("type")}')
    else:
        print(f"\n{fname}: NOT FOUND")
