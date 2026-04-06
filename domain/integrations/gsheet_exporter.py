"""
domain/integrations/gsheet_exporter.py
Google Sheets export — batched 22-column write.
"""
import json
import os
import re
import traceback
from datetime import date, datetime
from pathlib import Path

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_REPO_ROOT / ".env")
load_dotenv()

# Prefer reading sheet ID inside export_to_gsheet() so runtime env/.env matches Slack (not import-time only).
def _gsheet_id() -> str:
    return (os.getenv("GSHEET_ID") or os.getenv("GOOGLE_SHEET_ID") or "").strip()


GSHEET_ID = _gsheet_id()  # convenience for debug scripts; may be stale until re-import

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS_22 = [
    "Account",
    "OU",
    "Cloud AOV",
    "ATR",
    "Forecasted Attrition",
    "Util Rate",
    "Attrition Risk Reasons",
    "Red AC Flag",
    "Renewal Month",
    "Attrition Predictor",
    "Customer Success Score",
    "Adoption POV",
    "Health",
    "SF Products",
    "Risk Assessment",
    "Next Key Action",
    "AE",
    "Renewal Manager",
    "CSM",
    "Attrition Slack Channel",
    "Latest Commentary",
    "Exported At",
]


def _strip_slack_emoji(text: str) -> str:
    return re.sub(r":[a-z_]+:", "", str(text or "")).strip()


def _safe_cell(value) -> str:
    """Convert any value to a safe string for Google Sheets."""
    if value is None:
        return ""
    if isinstance(value, dict):
        return str(
            value.get("narrative")
            or value.get("cc_aov")
            or value.get("utilization_rate")
            or ""
        )
    if isinstance(value, (list, tuple)):
        return ", ".join(str(v) for v in value if v)
    if isinstance(value, bool):
        return "Yes" if value else "No"
    return str(value).strip()


def get_google_creds():
    """Google credentials from env (JSON string) or project-root service account files."""
    creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or os.getenv(
        "GSPREAD_CREDENTIALS"
    )
    if creds_json and creds_json.strip().startswith("{"):
        try:
            info = json.loads(creds_json)
            return Credentials.from_service_account_info(info, scopes=_SCOPES)
        except Exception:
            pass

    root = str(_REPO_ROOT)
    for fname in ("credentials.json", "google_creds.json"):
        fpath = os.path.join(root, fname)
        if os.path.isfile(fpath):
            return Credentials.from_service_account_file(fpath, scopes=_SCOPES)

    raise FileNotFoundError(
        "No Google credentials found. Set GOOGLE_SERVICE_ACCOUNT_JSON (JSON body) "
        "or place credentials.json in the project root."
    )


def export_to_gsheet(
    reviews: list,
    sheet_name: str | None = None,
    cloud: str | None = None,
) -> str:
    """
    Export GM reviews to Google Sheets — batched 22-column append.

    ``cloud``: workflow / user-selected cloud (slash command); drives AOV column
    header via ``cloud_aov_label`` — not Snowflake TARGET_CLOUD.

    Returns:
        Sheet URL, or "" on skip/failure.
    """
    from domain.analytics.snowflake_client import (
        cloud_aov_label,
        fmt_amount,
        get_sf_products_display,
    )
    from domain.salesforce.org62_client import get_account_team

    sheet_id = _gsheet_id()
    if not sheet_id:
        print(
            "⚠️ GSHEET_ID / GOOGLE_SHEET_ID not set — skipping Google Sheets export "
            "(set in .env and restart the bot if you just added it)."
        )
        return ""

    sheet_name = sheet_name or date.today().strftime("GM Review %Y-%m-%d")

    try:
        creds = get_google_creds()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(sheet_id)

        try:
            ws = sh.worksheet("Commerce Cloud GM Review")
        except Exception:
            try:
                ws = sh.worksheet(sheet_name)
            except Exception:
                ws = sh.add_worksheet(title=sheet_name, rows=500, cols=25)

        headers_row = list(HEADERS_22)
        if reviews:
            r0 = reviews[0]
            hdr_cloud = (cloud or r0.get("cloud") or "").strip()
            if not hdr_cloud:
                hdr_cloud = "Commerce Cloud"
            headers_row[2] = cloud_aov_label(hdr_cloud)
        else:
            headers_row[2] = "Cloud AOV"

        existing_row1: list = []
        try:
            existing_row1 = ws.row_values(1)
        except Exception:
            pass

        if existing_row1 != headers_row:
            if not existing_row1:
                ws.append_row(
                    headers_row, value_input_option="USER_ENTERED"
                )
                print("✓ Headers written to sheet")
            else:
                ws.insert_row(
                    headers_row,
                    index=1,
                    value_input_option="USER_ENTERED",
                    inherit_from_before=False,
                )
                print("✓ Headers inserted at row 1")
        else:
            print("✓ Headers already correct")

        exported_at = datetime.now().strftime("%Y-%m-%d %H:%M")
        all_rows: list[list] = []

        for review in reviews:
            account_name = review.get("account_name", "Unknown")
            account_id = review.get("account_id", "")
            opp = review.get("opp") or {}
            display = review.get("snowflake_display") or {}
            enrichment = review.get("enrichment") or {}
            red = review.get("red_account")
            all_prods = review.get("all_products_attrition") or []
            recommendation = review.get("recommendation", "")
            adoption_pov = review.get("adoption_pov") or ""
            if not adoption_pov or isinstance(adoption_pov, dict):
                adoption_pov = ""
            adoption_pov = str(adoption_pov).strip()

            ari_cat = display.get("ari_category", "Unknown")
            ari_prob = display.get("ari_probability", "N/A")

            renewal = enrichment.get("renewal_aov", {}) or {}
            ou = (
                renewal.get("csg_territory")
                or renewal.get("csg_geo")
                or "Unknown"
            )

            renewal_aov = float(
                enrichment.get("renewal_aov", {}).get("renewal_aov", 0) or 0
            )
            cc_aov = fmt_amount(renewal_aov) if renewal_aov > 0 else "Unknown"

            renewal_atr = float(
                enrichment.get("renewal_aov", {}).get("renewal_atr", 0) or 0
            )
            atr = fmt_amount(renewal_atr) if renewal_atr > 0 else "N/A"

            forecasted_atr = abs(float(opp.get("Forecasted_Attrition__c", 0) or 0))
            for_attrition = fmt_amount(forecasted_atr) if forecasted_atr > 0 else "N/A"

            util_rate = display.get("utilization_rate", "N/A")

            risk_reason = opp.get("License_At_Risk_Reason__c", "") or ""
            acv_reason = opp.get("ACV_Reason_Detail__c", "") or ""
            risk_reasons = risk_reason
            if acv_reason and acv_reason != risk_reason:
                risk_reasons = f"{risk_reasons} | {acv_reason}" if risk_reasons else acv_reason

            red_flag = "No"
            if red:
                red_stage = red.get("Stage__c", "")
                days_red = red.get("days_red")
                if days_red is None:
                    days_red = red.get("Days_Red__c") or 0
                try:
                    days_red = int(days_red) if days_red is not None else 0
                except (TypeError, ValueError):
                    days_red = 0
                days_red_str = f"{days_red} days" if days_red > 0 else "N/A"
                red_flag = f"Yes - {red_stage} ({days_red_str})"

            close_date = opp.get("CloseDate", "") or ""
            renewal_month = close_date[:7] if close_date else "N/A"

            ari_reason = display.get("ari_reason", "N/A")
            attrition_pred = f"{ari_cat} ({ari_prob}) - {ari_reason}"

            health_score = enrichment.get("health", {}).get("overall_score")
            health_literal = enrichment.get("health", {}).get(
                "overall_literal", "Unknown"
            )
            if health_score:
                try:
                    css_score = f"{int(float(health_score))} ({health_literal})"
                except (TypeError, ValueError):
                    css_score = "Unknown"
            else:
                css_score = "Unknown"

            health_display = _strip_slack_emoji(
                display.get("health_display", "Unknown")
            )
            sf_products = get_sf_products_display(all_prods)
            risk_assessment = recommendation

            next_step = opp.get("NextStep", "") or ""

            try:
                team = get_account_team(account_id) or {}
            except Exception:
                team = {}

            ae = team.get("ae", "") or ""
            renewal_mgr = team.get("renewal_mgr", "") or ""
            csm = team.get("csm", "") or ""

            slack_channel = ""

            specialist_notes = opp.get("Specialist_Sales_Notes__c", "") or ""
            description = opp.get("Description", "") or ""
            latest_update = ""
            if red:
                latest_update = red.get("Latest_Updates__c", "") or ""
            latest_commentary = specialist_notes or description or latest_update

            sf_url = f"https://org62.my.salesforce.com/{account_id}"
            account_name_escaped = str(account_name).replace('"', '""')
            acct_cell = f'=HYPERLINK("{sf_url}","{account_name_escaped}")'

            all_rows.append(
                [
                    _safe_cell(acct_cell),
                    _safe_cell(ou),
                    _safe_cell(cc_aov),
                    _safe_cell(atr),
                    _safe_cell(for_attrition),
                    _safe_cell(util_rate),
                    _safe_cell(risk_reasons),
                    _safe_cell(red_flag),
                    _safe_cell(renewal_month),
                    _safe_cell(attrition_pred),
                    _safe_cell(css_score),
                    _safe_cell(adoption_pov),
                    _safe_cell(health_display),
                    _safe_cell(sf_products),
                    _safe_cell(risk_assessment),
                    _safe_cell(next_step),
                    _safe_cell(ae),
                    _safe_cell(renewal_mgr),
                    _safe_cell(csm),
                    _safe_cell(slack_channel),
                    _safe_cell(latest_commentary),
                    _safe_cell(exported_at),
                ]
            )

        if all_rows:
            ws.append_rows(all_rows, value_input_option="USER_ENTERED")
            print(f"✓ Exported {len(all_rows)} rows to Google Sheets (batched)")

        sheet_url = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={ws.id}"
        )
        return sheet_url

    except Exception as e:
        msg = str(e).strip() or repr(e)
        print(f"❌ Google Sheets export error: {msg[:500]}")
        traceback.print_exc()
        return ""
