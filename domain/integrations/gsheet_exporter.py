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

# GM export column set (Snowflake = static renewal row; org62 opp = dynamic $ / notes).
HEADERS_22 = [
    "Account",
    "CSG_TERRITORY",
    "Cloud AOV",
    "ATR",
    "Forecasted Attrition",
    "Swing",
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
    "Manager Notes",
    "Next Steps",
    "AE",
    "Renewal Manager",
    "CSM",
    "Renewal Status",
    "Latest Commentary",
    "Lifecycle Stage",
    "Why Explanation",
    "Exported At",
]


def _strip_slack_emoji(text: str) -> str:
    return re.sub(r":[a-z_]+:", "", str(text or "")).strip()


def _sf_base_url() -> str:
    """Salesforce instance URL (no trailing slash)."""
    base = (
        os.getenv("SF_INSTANCE_URL")
        or os.getenv("SALESFORCE_INSTANCE_URL")
        or ""
    ).strip().rstrip("/")
    return base if base else "https://org62.my.salesforce.com"


def _sf_opportunity_url(opp_id: str) -> str:
    oid = str(opp_id or "").strip()
    if not oid:
        return ""
    return f"{_sf_base_url()}/{oid}"


def _opp_owner_name(opp: dict) -> str:
    o = (opp or {}).get("Owner")
    if isinstance(o, dict):
        return str(o.get("Name") or "").strip()
    return ""


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


def _adapt_bulk_row_for_sheet(bulk_row: dict) -> dict:
    """
    Map run_bulk_gm_review() row shape -> export_to_gsheet() expected shape.
    """
    opp_id = bulk_row.get("opportunity_id") or ""
    atr_val = bulk_row.get("atr") or 0
    fcast_val = bulk_row.get("forecasted_attrition") or 0
    cc_aov_val = bulk_row.get("cc_aov") or 0
    util = bulk_row.get("utilization_rate") or "N/A"
    territory = bulk_row.get("territory") or ""
    close_date = bulk_row.get("close_date") or ""
    stage = bulk_row.get("stage") or ""
    risk_cat = bulk_row.get("risk_category") or "Unknown"
    risk_notes = bulk_row.get("risk_detail") or ""
    red_notes = bulk_row.get("red_notes") or ""
    days_red = bulk_row.get("days_red") or 0
    sf_products = bulk_row.get("sf_products") or ""
    swing = bulk_row.get("swing") or 0
    ae = bulk_row.get("ae") or ""
    renewal_mgr = bulk_row.get("renewal_manager") or ""
    csm = bulk_row.get("csm") or ""
    renewal_status = bulk_row.get("renewal_status") or ""
    next_steps = bulk_row.get("next_steps") or ""
    manager_notes = bulk_row.get("manager_notes") or ""

    renewal_month = close_date[:7] if close_date else ""
    is_red = bool(days_red or red_notes)
    red_account = (
        {
            "Stage__c": "Red",
            "days_red": days_red,
            "Latest_Updates__c": red_notes,
            "Days_Red__c": days_red,
        }
        if is_red
        else None
    )

    return {
        "account_name": bulk_row.get("account") or "Unknown",
        "account_id": bulk_row.get("account_id") or "",
        "opportunity_id": opp_id,
        "opp": {
            "Id": opp_id,
            "CloseDate": close_date,
            "StageName": stage,
            "Forecasted_Attrition__c": fcast_val,
            "License_At_Risk_Reason__c": risk_notes,
            "Swing__c": swing,
            "PAM_Comment__c": manager_notes,
            "NextStep": next_steps,
        },
        "snowflake_display": {
            "utilization_rate": util,
            "csg_territory": territory,
            "ari_category": risk_cat,
            "ari_probability": "N/A",
            "ari_reason": risk_notes,
            "health_display": "Unknown",
            "renewal_aov": {
                "renewal_aov": cc_aov_val,
                "renewal_atr_snow": atr_val,
                "csg_territory": territory,
                "renewal_close_month": renewal_month,
            },
        },
        "enrichment": {
            "renewal_aov": {
                "renewal_aov": cc_aov_val,
                "csg_territory": territory,
                "renewal_close_month": renewal_month,
                "ae_name": ae,
                "renewal_manager": renewal_mgr,
                "csm_name": csm,
                "renewal_status": renewal_status,
            },
            "health": {
                "overall_score": None,
                "overall_literal": "Unknown",
            },
            "usage": {
                "utilization_rate": util,
            },
        },
        "red_account": red_account,
        "all_products_attrition": [],
        "recommendation": risk_notes,
        "adoption_pov": "",
        "sf_products_direct": bulk_row.get("sf_products") or "",
        "adoption_pov_direct": bulk_row.get("adoption_pov") or "",
    }


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
    Export GM reviews to Google Sheets — batched fixed-column append (see ``HEADERS_22``).

    ``cloud``: workflow / user-selected cloud (slash command); drives AOV column
    header via ``cloud_aov_label`` — not Snowflake TARGET_CLOUD.

    Returns:
        Sheet URL, or "" on skip/failure.
    """
    from domain.analytics.snowflake_client import (
        calculate_overall_ari,
        fmt_amount,
        get_sf_products_display,
        resolve_money,
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

    # Bulk workflow rows use a different shape than per-account workflow rows.
    if reviews and "account_name" not in (reviews[0] or {}) and "account" in (reviews[0] or {}):
        reviews = [_adapt_bulk_row_for_sheet(r) for r in reviews]

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
            adoption_pov = (
                review.get("adoption_pov_direct")
                or review.get("adoption_pov")
                or ""
            )
            if not adoption_pov or isinstance(adoption_pov, dict):
                adoption_pov = ""
            adoption_pov = str(adoption_pov).strip()

            ari_cat = display.get("ari_category", "Unknown")
            ari_prob = display.get("ari_probability", "N/A")
            ari_reason = display.get("ari_reason", "N/A")

            if (ari_cat == "Unknown" or ari_prob == "N/A") and all_prods:
                try:
                    ov = calculate_overall_ari(all_prods, min_atr_threshold=0)
                    oc = str(ov.get("category") or "").strip()
                    if oc and oc.lower() != "unknown":
                        ari_cat = ov["category"]
                        prob = ov.get("probability")
                        if prob is not None:
                            try:
                                pf = float(prob)
                                ari_prob = (
                                    f"{pf * 100:.1f}%"
                                    if pf <= 1.0
                                    else f"{pf:.1f}%"
                                )
                            except (TypeError, ValueError):
                                pass
                        ar0 = ov.get("reason")
                        if ar0:
                            ari_reason = ar0
                except Exception:
                    pass

            renewal = enrichment.get("renewal_aov", {}) or {}
            if not isinstance(renewal, dict):
                renewal = {}
            # CSG_TERRITORY only (e.g. AMER REG) — not CSG_GEO / area
            csg_territory_cell = (
                (renewal.get("csg_territory") or "").strip()
                or (display.get("csg_territory") or "").strip()
                or "Unknown"
            )

            # AOV / ATR / swing — same resolution as canvas (Snowflake + display + opp)
            cc_aov = resolve_money(display, opp, "aov")
            atr = resolve_money(display, opp, "atr")

            fcast_raw = float(opp.get("Forecasted_Attrition__c") or 0)
            fcast_cell = (
                fmt_amount(abs(fcast_raw)) if fcast_raw else "N/A"
            )
            if fcast_cell == "N/A":
                fcast_cell = resolve_money(display, opp, "attrition")

            swing_cell = resolve_money(display, opp, "swing")

            util_rate = display.get("utilization_rate", "N/A")
            if util_rate in ("N/A", "Unknown", "", None):
                util_rate = (enrichment.get("usage") or {}).get(
                    "utilization_rate", "N/A"
                )

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
            renewal_month = (
                str(renewal.get("renewal_close_month") or "").strip()
                or (close_date[:7] if close_date else "")
                or "N/A"
            )

            attrition_pred = f"{ari_cat} ({ari_prob}) - {ari_reason}"

            health_score = enrichment.get("health", {}).get("overall_score")
            health_literal = enrichment.get("health", {}).get(
                "overall_literal", "Unknown"
            )
            if health_score is not None and str(health_score).strip() != "":
                try:
                    css_score = f"{int(float(health_score))} ({health_literal})"
                except (TypeError, ValueError):
                    css_score = "Unknown"
            else:
                css_score = "Unknown"

            health_display = _strip_slack_emoji(
                display.get("health_display", "Unknown")
            )
            sf_products = (
                review.get("sf_products_direct")
                or get_sf_products_display(all_prods)
                or "N/A"
            )
            risk_assessment = recommendation

            manager_notes = str(opp.get("PAM_Comment__c") or "").strip()
            next_step = opp.get("NextStep", "") or ""

            try:
                team = get_account_team(account_id) or {}
            except Exception:
                team = {}

            ae = (
                (renewal.get("ae_name") or "").strip()
                or team.get("ae", "")
                or _opp_owner_name(opp)
                or "Unknown"
            )
            renewal_mgr = (
                (renewal.get("renewal_manager") or "").strip()
                or team.get("renewal_mgr", "")
                or "Unknown"
            )
            csm = (renewal.get("csm_name") or "").strip() or team.get("csm", "") or "Unknown"
            renewal_status = (renewal.get("renewal_status") or "").strip() or "Unknown"

            specialist_notes = opp.get("Specialist_Sales_Notes__c", "") or ""
            description = opp.get("Description", "") or ""
            latest_update = ""
            if red:
                latest_update = red.get("Latest_Updates__c", "") or ""
            latest_commentary = specialist_notes or description or latest_update
            lifecycle_stage = review.get("lifecycle_stage") or "Unknown"
            why_explanation = str(
                (review.get("why_explanation") or {}).get("primary_reason") or ""
            )

            opp_id_link = str(
                review.get("opportunity_id")
                or (opp or {}).get("Id")
                or ""
            ).strip()
            account_name_escaped = str(account_name).replace('"', '""')
            if opp_id_link:
                sf_url = _sf_opportunity_url(opp_id_link)
                acct_cell = f'=HYPERLINK("{sf_url}","{account_name_escaped}")'
            else:
                acct_cell = account_name_escaped

            all_rows.append(
                [
                    _safe_cell(acct_cell),
                    _safe_cell(csg_territory_cell),
                    _safe_cell(cc_aov),
                    _safe_cell(atr),
                    _safe_cell(fcast_cell),
                    _safe_cell(swing_cell),
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
                    _safe_cell(manager_notes),
                    _safe_cell(next_step),
                    _safe_cell(ae),
                    _safe_cell(renewal_mgr),
                    _safe_cell(csm),
                    _safe_cell(renewal_status),
                    _safe_cell(latest_commentary),
                    _safe_cell(lifecycle_stage),
                    _safe_cell(why_explanation),
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
