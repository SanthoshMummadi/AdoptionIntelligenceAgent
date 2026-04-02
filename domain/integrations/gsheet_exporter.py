"""
Batched Google Sheets export for GM reviews (service account).
"""
import os
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

_SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)


def _credentials_path() -> str:
    env = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or os.getenv("GSPREAD_CREDENTIALS")
    if env:
        return env
    root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    for name in ("credentials.json", "google_creds.json"):
        path = os.path.join(root, name)
        if os.path.isfile(path):
            return path
    return os.path.join(root, "credentials.json")


def _hyperlink_formula(url: str, label: str) -> str:
    u = str(url).replace('"', '""')
    lab = str(label).replace('"', '""')
    return f'=HYPERLINK("{u}", "{lab}")'


def export_to_gsheet(reviews: list, sheet_url: str) -> int:
    """
    Append GM review rows in one batched request (USER_ENTERED so HYPERLINK formulas work).

    Args:
        reviews: List of dicts with keys account, opp, snowflake_display, red_account,
            risk_notes, recommendation.
        sheet_url: Full spreadsheet URL or spreadsheet ID.

    Returns:
        Number of rows appended.
    """
    path = _credentials_path()
    creds = Credentials.from_service_account_file(path, scopes=list(_SCOPES))
    client = gspread.authorize(creds)

    if sheet_url.startswith("http"):
        sheet = client.open_by_url(sheet_url)
    else:
        sheet = client.open_by_key(sheet_url.strip())

    worksheet = sheet.get_worksheet(0)

    headers = [
        "Account Name",
        "Account ID",
        "Opportunity ID",
        "Opportunity Name",
        "ARI Category",
        "ARI Probability",
        "ARI Reason",
        "Health Score",
        "Health Literal",
        "Cloud AOV",
        "Utilization Rate",
        "GMV Rate",
        "Forecasted Attrition",
        "Close Date",
        "Stage",
        "Risk Assessment",
        "Recommendations",
        "Product Breakdown",
        "Red Account Stage",
        "Days Red",
        "Region",
        "Salesforce Link",
    ]

    existing = worksheet.row_values(1)
    if not existing or len(existing) < len(headers):
        worksheet.update("A1:V1", [headers], value_input_option="USER_ENTERED")

    all_rows: list[list[Any]] = []

    for review in reviews:
        acct = review.get("account") or {}
        opp = review.get("opp") or {}
        snow = review.get("snowflake_display") or {}
        red = review.get("red_account") or {}

        account_name = acct.get("name", "Unknown")
        account_id = acct.get("id", "")
        sf_link = f"https://org62.my.salesforce.com/{account_id}"

        row = [
            _hyperlink_formula(sf_link, account_name),
            account_id,
            opp.get("Id", ""),
            opp.get("Name", ""),
            snow.get("ari_category", "Unknown"),
            snow.get("ari_probability", "N/A"),
            snow.get("ari_reason", "N/A"),
            snow.get("health_score", ""),
            snow.get("health_literal", "Unknown"),
            snow.get("cc_aov", "Unknown"),
            snow.get("utilization_rate", "N/A"),
            snow.get("gmv_rate", "N/A"),
            abs(opp.get("Forecasted_Attrition__c") or 0),
            opp.get("CloseDate", ""),
            opp.get("StageName", ""),
            (review.get("risk_notes", "") or "")[:100],
            (review.get("recommendation", "") or "")[:100],
            "",
            red.get("Stage__c", "") if red else "",
            red.get("Days_Red__c", "") if red else "",
            acct.get("billing_country", ""),
            sf_link,
        ]
        all_rows.append(row)

    if all_rows:
        worksheet.append_rows(all_rows, value_input_option="USER_ENTERED")

    return len(all_rows)
