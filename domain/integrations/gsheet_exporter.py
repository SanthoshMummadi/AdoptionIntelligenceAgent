"""
domain/integrations/gsheet_exporter.py
Google Sheets export — batched GM Review columns (see ``HEADERS_22`` / ``GM_REVIEW_SHEET_HEADERS``).
"""
import json
import os
import re
import traceback
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional

import gspread

from log_utils import log_debug
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

try:
    from google.auth.exceptions import RefreshError
except ImportError:  # pragma: no cover
    RefreshError = type("RefreshError", (Exception,), {})  # type: ignore[misc, assignment]

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_HOW_VERIFY_CREDS_CMD = "python3 get_sa_email.py"
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
    "Burn Rate",
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
    "At-Risk Classification",
    "Outreach Status",
    "Protect Channel",
]

GM_REVIEW_SHEET_HEADERS = HEADERS_22

_PROTECT_CHANNEL_COLUMN_MIN_COLS = 31


def ensure_worksheet_min_columns(ws, min_cols: int = _PROTECT_CHANNEL_COLUMN_MIN_COLS) -> None:
    """
    Resize tab so columns like Col AC exist (protect channel id/name). Harmless no-op when wide enough.

    Covers both new exports and `/initiate-outreach` scans that bypass ``export_to_gsheet``.
    """
    if ws is None:
        return
    try:
        cur_cols = getattr(ws, "col_count", None)
        if cur_cols is not None and cur_cols >= min_cols:
            return
        rows_n = getattr(ws, "row_count", None) or 500
        ws.resize(rows=max(rows_n, 500), cols=min_cols)
    except Exception as e:
        print(
            f"⚠️ Sheet resize cols>={min_cols} failed: {str(e)[:240]}"
        )


# Column AA (27th column, 1-based) — Stage 2 V6 classification dropdown
CLASSIFICATION_COL_1BASE = 27
CLASSIFICATION_CELL_PREFIX = "AA"
CLASSIFICATION_PENDING = "Pending Review"

# Col AB — outreach status (manual GM); data validation list
OUTREACH_STATUS_VALUES = (
    "Reviewed",
    "Wait for CSG",
    "Outreach Initiated",
    "No Action Needed",
)


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


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_burn_rate_from_sheet_cell(raw: str) -> float | None:
    """Leading float from cells like ``1.12 (89.5% / 80.2%)`` or legacy util ``45.2%``."""
    s = (raw or "").strip()
    if not s or s.upper() == "N/A":
        return None
    m = re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


# Display order when keys match Commerce burn buckets from bulk_cidm.
_BURN_SHEET_LABEL_ORDER = ("B2C PPO", "B2C GMV", "B2B", "OMS")


def _format_burn_rate_cell(
    burn_rate: Any,
    util_pct: Any,
    time_elapsed_pct: Any,
    by_l2: Any,
) -> str:
    """Burn index only, plus every labeled util% bucket — no blended util/elapsed parentheses."""
    if burn_rate in (None, "", "N/A") or (
        isinstance(burn_rate, str) and str(burn_rate).strip().upper() == "N/A"
    ):
        return "N/A"
    try:
        br = float(burn_rate)
    except (TypeError, ValueError):
        return "N/A"
    head = f"{br:.2f}"
    if not isinstance(by_l2, dict) or len(by_l2) == 0:
        return head

    def _burn_label_sort(k: object) -> tuple:
        label = str(k or "").strip()
        try:
            return (0, _BURN_SHEET_LABEL_ORDER.index(label))
        except ValueError:
            return (1, label.lower())

    parts: list[str] = []
    for label_key in sorted(by_l2.keys(), key=_burn_label_sort):
        d = by_l2[label_key]
        if not isinstance(d, dict):
            continue
        up = d.get("util_pct")
        if up is None:
            continue
        try:
            uf = float(up)
        except (TypeError, ValueError):
            continue
        lbl = str(label_key or "").strip() or "L2"
        parts.append(f"{lbl}: {uf:.1f}%")
    if not parts:
        return head
    return head + " | " + " | ".join(parts)


def _red_ac_flag_sheet_cell(red_ac_flag: str) -> str:
    """Red AC column: plain text, or Sheets HYPERLINK when value is a URL."""
    v = str(red_ac_flag or "").strip()
    if not v:
        return ""
    if v.lower().startswith("http"):
        esc = v.replace('"', '""')
        return f'=HYPERLINK("{esc}","Red Account")'
    return v


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
    fcast_val = abs(
        _to_float(
            bulk_row.get("forecasted_atr")
            or bulk_row.get("forecasted_attrition")
            or 0
        )
    )
    cc_aov_val = bulk_row.get("cc_aov") or 0
    util = bulk_row.get("utilization_rate") or "N/A"
    territory = bulk_row.get("territory") or ""
    close_date = bulk_row.get("close_date") or ""
    stage = bulk_row.get("stage") or ""
    risk_cat = bulk_row.get("risk_category") or "Unknown"
    risk_notes = bulk_row.get("risk_detail") or ""
    red_notes = bulk_row.get("red_notes") or ""
    red_issue_for_red = str(
        bulk_row.get("red_issue_product")
        or bulk_row.get("Issue_Product__c")
        or ""
    ).strip()
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
            "Issue_Product__c": red_issue_for_red,
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
        "red_ac_flag": bulk_row.get("red_ac_flag") or "",
        "burn_rate": bulk_row.get("burn_rate"),
        "predicted_eoc_util": bulk_row.get("predicted_eoc_util"),
        "burn_rate_by_l2": bulk_row.get("burn_rate_by_l2") or {},
        "burn_rate_status": bulk_row.get("burn_rate_status"),
        "burn_util_pct_overall": bulk_row.get("burn_util_pct_overall"),
        "burn_time_elapsed_pct": bulk_row.get("burn_time_elapsed_pct"),
    }


def _ensure_service_account_key_usable(info: dict[str, Any]) -> None:
    """
    Fail fast before calling Google Sheets (invalid_grant / Invalid JWT Signature is
    opaque). Does not print key material.
    """
    pk = info.get("private_key") if isinstance(info, dict) else None
    pk = str(pk or "")
    mail = str((info.get("client_email") or "")).strip()
    pid = str((info.get("project_id") or "")).strip()

    placeholder_markers = (
        "REPLACE_",
        "PASTE_",
        "YOUR_",
        "example.invalid",
        "REPLACE_EMAIL",
        "REPLACE_PROJECT",
    )
    blob = pk + mail + pid
    if any(m in blob for m in placeholder_markers) or pid == "REPLACE_PROJECT_ID":
        raise ValueError(
            "Google service-account JSON looks like the template placeholders. Replace "
            f"{_REPO_ROOT / 'google_creds.json'} (or legacy credentials.json) with the JSON key "
            "downloaded from Google Cloud Console → IAM → Service Accounts → Keys → Add key "
            "(do not invent or shorten the private_key PEM)."
        )
    if (
        "BEGIN PRIVATE KEY" not in pk
        and "BEGIN RSA PRIVATE KEY" not in pk
    ):
        raise ValueError(
            "service_account private_key missing a PEM header (BEGIN PRIVATE KEY). "
            "Use the untouched JSON downloaded from GCP."
        )
    try:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        serialization.load_pem_private_key(pk.encode(), password=None, backend=default_backend())
    except ImportError:
        pass
    except Exception as e:
        hint = ""
        if "could not deserialize" in str(e).lower() or "invalid" in str(e).lower():
            hint = " Fix: re-download the key from GCP or fix \\n escapes in private_key JSON."
        raise ValueError(f"Could not parse private_key PEM ({e!s}).{hint}") from e


def _creds_from_service_account_info(info: dict[str, Any]) -> Credentials:
    _ensure_service_account_key_usable(info)
    return Credentials.from_service_account_info(info, scopes=_SCOPES)


def get_google_creds():
    """Google credentials from env (JSON string) or project-root service account files."""
    creds_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or os.getenv(
        "GSPREAD_CREDENTIALS"
    )
    if creds_json and creds_json.strip().startswith("{"):
        try:
            info = json.loads(creds_json)
            return _creds_from_service_account_info(info)
        except ValueError:
            raise
        except Exception:
            pass

    root = str(_REPO_ROOT)
    for fname in ("google_creds.json", "credentials.json"):
        fpath = os.path.join(root, fname)
        if os.path.isfile(fpath):
            with open(fpath, encoding="utf-8") as f:
                info = json.load(f)
            return _creds_from_service_account_info(info)

    raise FileNotFoundError(
        "No Google credentials found for Sheets. Set GOOGLE_SERVICE_ACCOUNT_JSON "
        "(full service-account JSON) or put google_creds.json (or credentials.json) in "
        f"{_REPO_ROOT}; share the spreadsheet with that service-account email "
        "(see get_sa_email.py if you want the address)."
    )


def export_to_gsheet(
    reviews: list,
    sheet_name: str | None = None,
    cloud: str | None = None,
    slack_client: Optional[Any] = None,
) -> str:
    """
    Export GM reviews to Google Sheets — batched fixed-column append (see ``HEADERS_22``).

    ``cloud``: workflow / user-selected cloud (slash command); drives AOV column
    header via ``cloud_aov_label`` — not Snowflake TARGET_CLOUD.

    ``slack_client``: when set (e.g. Bolt ``WebClient``), runs Stage 2 V6 classification
    on newly appended rows (see ``services.gm_review_bulk_workflow._run_classification_pass``).
    Disabled when ``GM_REVIEW_STAGE2_AUTO_CLASSIFY`` is ``0`` / ``false``.

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
                ws = sh.add_worksheet(title=sheet_name, rows=500, cols=31)

        ensure_worksheet_min_columns(ws, _PROTECT_CHANNEL_COLUMN_MIN_COLS)

        headers_row = list(GM_REVIEW_SHEET_HEADERS)
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
        classification_rows: list[dict] = []

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

            burn_rate_cell = _format_burn_rate_cell(
                review.get("burn_rate"),
                review.get("burn_util_pct_overall"),
                review.get("burn_time_elapsed_pct"),
                review.get("burn_rate_by_l2"),
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

            raw_red_ac = str(review.get("red_ac_flag") or "").strip()
            red_ac_sheet = (
                _red_ac_flag_sheet_cell(raw_red_ac) if raw_red_ac else red_flag
            )

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

            fcast_numeric = abs(float(opp.get("Forecasted_Attrition__c") or 0))
            swing_numeric = float(opp.get("Swing__c") or 0)
            renewal_for_classify = renewal_month
            if renewal_for_classify in ("N/A", "", None):
                renewal_for_classify = (
                    close_date[:7] if len(close_date) >= 7 else ""
                )
            br_classify = review.get("burn_rate")
            try:
                burn_rate_numeric = (
                    float(br_classify)
                    if br_classify not in (None, "", "N/A")
                    and str(br_classify).strip().upper() != "N/A"
                    else None
                )
            except (TypeError, ValueError):
                burn_rate_numeric = None

            classification_rows.append(
                {
                    "opp_id": opp_id_link,
                    "account_nm": account_name,
                    "csg_territory": csg_territory_cell,
                    "forecasted_attrition": fcast_numeric,
                    "swing_amount": swing_numeric,
                    "renewal_month": renewal_for_classify or "",
                    "account_id": account_id,
                    "burn_rate": burn_rate_numeric,
                    "red_ac_flag": red_flag,
                }
            )

            all_rows.append(
                [
                    _safe_cell(acct_cell),
                    _safe_cell(csg_territory_cell),
                    _safe_cell(cc_aov),
                    _safe_cell(atr),
                    _safe_cell(fcast_cell),
                    _safe_cell(swing_cell),
                    _safe_cell(burn_rate_cell),
                    _safe_cell(risk_reasons),
                    _safe_cell(red_ac_sheet),
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
                    _safe_cell(CLASSIFICATION_PENDING),
                    _safe_cell(""),
                    _safe_cell(""),
                ]
            )

        stage2_auto = slack_client is not None and (
            os.getenv("GM_REVIEW_STAGE2_AUTO_CLASSIFY", "1").lower()
            not in ("0", "false", "no", "off")
        )

        if all_rows:
            start_row_idx = len(ws.get_all_values()) + 1
            ws.append_rows(all_rows, value_input_option="USER_ENTERED")
            print(f"✓ Exported {len(all_rows)} rows to Google Sheets (batched)")

            if stage2_auto:
                try:
                    from domain.salesforce.org62_client import Org62Client

                    from services.gm_review_bulk_workflow import (
                        _run_classification_pass,
                    )

                    org62_client = Org62Client()
                    _run_classification_pass(
                        slack_client,
                        org62_client,
                        ws,
                        classification_rows,
                        start_row_index=start_row_idx,
                    )
                    print(f"✓ Stage 2 classification applied to {len(classification_rows)} row(s)")
                except Exception as e:
                    msg = str(e).strip() or repr(e)
                    print(f"⚠️ Stage 2 classification failed (non-fatal): {msg[:300]}")
                    traceback.print_exc()
            else:
                try:
                    apply_classification_dropdown(ws)
                except Exception as e:
                    print(
                        f"⚠️ Classification dropdown not applied: {str(e)[:200]}"
                    )
                try:
                    apply_outreach_status_dropdown(ws)
                except Exception as e:
                    print(
                        f"⚠️ Outreach status dropdown not applied: {str(e)[:200]}"
                    )
                if slack_client is not None:
                    try:
                        from services.stage3_outreach import (
                            scan_sheet_for_outreach,
                        )

                        c3 = scan_sheet_for_outreach(slack_client, ws)
                        if c3 > 0:
                            log_debug(
                                f"Stage 3: {c3} outreach(es) initiated (post-export)"
                            )
                    except Exception as ex:
                        print(
                            f"⚠️ Stage 3 outreach scan failed: {str(ex)[:240]}"
                        )

        sheet_url = (
            f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit#gid={ws.id}"
        )
        return sheet_url

    except RefreshError as e:
        msg = str(e).strip().lower()
        print(
            "❌ Google Sheets auth failed while refreshing the access token. "
            f"Underlying error: {str(e)[:400]}"
        )
        if "invalid jwt signature" in msg or ("jwt" in msg and "signature" in msg):
            print(
                "   → Typical causes: (1) google_creds.json is still the template or a truncated "
                "paste — replace with the IAM **JSON key** GCP gave you unchanged; "
                "(2) you replaced the service account — create a **new key** on that SA and "
                "update the JSON; "
                "(3) private_key PEM line breaks corrupted — paste the downloaded file verbatim."
            )
        print(f"   → Service-account email for sharing the sheet: run `{_HOW_VERIFY_CREDS_CMD}`")
        traceback.print_exc()
        return ""

    except Exception as e:
        msg = str(e).strip() or repr(e)
        print(f"❌ Google Sheets export error: {msg[:500]}")
        traceback.print_exc()
        return ""



def apply_classification_dropdown(worksheet) -> None:
    """Set Col AA (AA2:AA1000) V6 dropdown. Call once per export / refresh."""
    try:
        from gspread_formatting import (
            BooleanCondition,
            DataValidationRule,
            set_data_validation_for_cell_range,
        )
    except ImportError:
        import logging

        logging.getLogger(__name__).warning(
            "install gspread-formatting for Col AA dropdown (pip install gspread-formatting)"
        )
        return

    from services.classify_renewal_workflow import CLASSIFICATION_VALUES

    rule = DataValidationRule(
        BooleanCondition("ONE_OF_LIST", list(CLASSIFICATION_VALUES)),
        showCustomUi=True,
        strict=True,
    )
    set_data_validation_for_cell_range(worksheet, "AA2:AA1000", rule)


def apply_outreach_status_dropdown(worksheet) -> None:
    """Set Col AB (AB2:AB1000) dropdown for Outreach Status (manual GM entry)."""
    try:
        from gspread_formatting import (
            BooleanCondition,
            DataValidationRule,
            set_data_validation_for_cell_range,
        )
    except ImportError:
        import logging

        logging.getLogger(__name__).warning(
            "install gspread-formatting for Col AB outreach dropdown "
            "(pip install gspread-formatting)"
        )
        return

    rule = DataValidationRule(
        BooleanCondition("ONE_OF_LIST", list(OUTREACH_STATUS_VALUES)),
        showCustomUi=True,
        strict=True,
    )
    set_data_validation_for_cell_range(worksheet, "AB2:AB1000", rule)


def batch_write_classifications(
    worksheet,
    start_row_index: int,
    classifications: list[str],
) -> None:
    """
    Write Col AA for a contiguous block of sheet rows in one ``values.batchUpdate``
    via ``batch_update``, and apply background tints in a single ``format_cell_ranges``.
    """
    if not classifications:
        return
    prefix = CLASSIFICATION_CELL_PREFIX
    end_row_index = start_row_index + len(classifications) - 1
    rng = f"{prefix}{start_row_index}:{prefix}{end_row_index}"
    worksheet.batch_update(
        [{"range": rng, "values": [[c] for c in classifications]}],
        value_input_option="USER_ENTERED",
    )
    try:
        from gspread_formatting import CellFormat, Color, format_cell_ranges
    except ImportError:
        return
    color_map = {
        "Actionable": Color(0.72, 0.88, 0.80),
        "Actionable — AOVPP": Color(0.72, 0.88, 0.80),
        "Actionable — Renewals + Product": Color(0.72, 0.88, 0.80),
        "Actionable — URGENT": Color(0.965, 0.322, 0.102),
        "Non-Actionable — Signed with Competitor": Color(0.96, 0.80, 0.80),
        "Non-Actionable — Already Migrating": Color(0.957, 0.800, 0.800),
        "Non-Actionable — KMOD": Color(0.96, 0.80, 0.80),
        "Non-Actionable — Macro / M&A": Color(0.96, 0.80, 0.80),
        "Non-Actionable — Miscellaneous": Color(0.96, 0.80, 0.80),
        "Already Attrited": Color(0.99, 0.91, 0.70),
        "Pending Review": Color(0.94, 0.94, 0.94),
    }
    specs: list[tuple[str, Any]] = []
    for offset, classification in enumerate(classifications):
        row_idx = start_row_index + offset
        cell = f"{prefix}{row_idx}"
        tint = color_map.get(classification, Color(1, 1, 1))
        specs.append((cell, CellFormat(backgroundColor=tint)))
    format_cell_ranges(worksheet, specs)


def write_classification(worksheet, row_index: int, classification: str) -> None:
    """Write classification text + tint background for Col AA (27)."""
    batch_write_classifications(worksheet, row_index, [classification])


def write_classification_sheet_cell(
    worksheet, row_index: int, classification: str
) -> None:
    """Backward-compatible alias for ``write_classification``."""
    write_classification(worksheet, row_index, classification)


def parse_gsheet_id_from_url(text: str) -> str | None:
    m = re.search(
        r"/spreadsheets/d/([a-zA-Z0-9-_]+)",
        text or "",
    )
    return m.group(1) if m else None


def _extract_opp_id_from_account_cell(acct_cell: str) -> str:
    raw = acct_cell or ""
    m = re.search(r"/(006[A-Za-z0-9]{12,18})(?:/[?]?|$)", raw)
    if m:
        return m.group(1).strip()
    m2 = re.search(r"\b006[A-Za-z0-9]{12,18}\b", raw)
    return (m2.group(0).strip()) if m2 else ""


def _parse_hyperlink_account_label(acct_cell: str) -> str:
    s = acct_cell or ""
    m = re.search(r'HYPERLINK\s*\(\s*"[^"]*"\s*,\s*"((?:[^"\\]|\\.)*)"', s, re.I)
    if m:
        return m.group(1).replace('""', '"').strip()
    return s.strip()


def _money_cell_to_float(cell: str) -> float:
    if not cell or str(cell).strip().upper() in ("N/A", ""):
        return 0.0
    s = str(cell).replace(",", "").replace("$", "").strip()
    try:
        if s.endswith("M") or s.endswith("m"):
            return float(s[:-1].strip()) * 1_000_000
        if s.endswith("K") or s.endswith("k"):
            return float(s[:-1].strip()) * 1_000
        return float(s)
    except (TypeError, ValueError):
        try:
            return float(re.sub(r"[^0-9.+-]", "", s) or 0)
        except (TypeError, ValueError):
            return 0.0


class GSheetExporter:
    """
    Read/write GM Review commerce sheet rows for `/classify-renewal`.

    Requires ``GSHEET_ID`` / ``GOOGLE_SHEET_ID`` and the commerce tab title
    (default ``Commerce Cloud GM Review``), unless a sheet ID is overridden.
    """

    def __init__(
        self,
        sheet_id: str | None = None,
        worksheet_title: str | None = None,
    ):
        sid = (sheet_id or "").strip() or _gsheet_id()
        self.sheet_id = sid
        self.worksheet_title = (
            worksheet_title
            or os.getenv(
                "GM_REVIEW_GOOGLE_TAB",
                "Commerce Cloud GM Review",
            ).strip()
        )
        self._ws = None

    def _worksheet(self):
        if self._ws is not None:
            return self._ws
        if not self.sheet_id:
            raise ValueError(
                "Missing GSHEET_ID / GOOGLE_SHEET_ID (or constructor sheet_id)."
            )
        creds = get_google_creds()
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(self.sheet_id)
        try:
            self._ws = sh.worksheet(self.worksheet_title)
        except Exception:
            self._ws = sh.sheet1
        return self._ws

    def _header_index(self) -> dict[str, int]:
        ws = self._worksheet()
        row1 = ws.row_values(1)
        return {h.strip(): i for i, h in enumerate(row1) if h}

    def _row_dict(
        self,
        values: list[str],
        sheet_row_index: int,
        header_to_idx: dict[str, int] | None = None,
    ) -> dict:
        def col(name: str, default_idx: int) -> str:
            if header_to_idx and name in header_to_idx:
                i = header_to_idx[name]
            else:
                i = default_idx
            return (values[i] if i < len(values) else "").strip()

        account_cell = col("Account", 0)
        opp_id = _extract_opp_id_from_account_cell(account_cell)
        account_nm = _parse_hyperlink_account_label(account_cell)
        renewal_raw = col("Renewal Month", 9)

        cls_idx = (
            header_to_idx.get("At-Risk Classification")
            if header_to_idx
            else None
        )
        if cls_idx is None:
            cls_idx = CLASSIFICATION_COL_1BASE - 1
        classification_val = (
            values[cls_idx].strip() if cls_idx < len(values) else ""
        )

        swing_val = col("Swing", 5)
        fcast_val = col("Forecasted Attrition", 4)
        burn_raw = col("Burn Rate", 6)
        if not burn_raw:
            burn_raw = col("Util Rate", 6)
        red_raw = col("Red AC Flag", 8)

        return {
            "opp_id": opp_id,
            "account_nm": account_nm or account_cell,
            "csg_territory": col("CSG_TERRITORY", 1),
            "forecasted_attrition": _money_cell_to_float(fcast_val),
            "swing_amount": _money_cell_to_float(swing_val),
            "renewal_month": renewal_raw or "",
            "sheet_row_index": sheet_row_index,
            "account_id": "",
            "_classification_cell": classification_val,
            "burn_rate": _parse_burn_rate_from_sheet_cell(burn_raw),
            "red_ac_flag": red_raw or "",
        }

    def _data_rows_with_header(self):
        ws = self._worksheet()
        all_vals = ws.get_all_values()
        if not all_vals:
            return [], {}
        headers = [h.strip() for h in all_vals[0]]
        header_map = {h: i for i, h in enumerate(headers) if h}
        return all_vals[1:], header_map

    def find_row_by_opp_id(self, opp_id: str) -> dict | None:
        want = (opp_id or "").strip()
        if not want:
            return None
        rows, hm = self._data_rows_with_header()
        for i, vals in enumerate(rows, start=2):
            rd = self._row_dict(vals, i, hm)
            if rd["opp_id"] == want:
                return rd
        return None

    def find_rows_by_name(self, name: str) -> list[dict]:
        q = (name or "").strip().lower()
        if not q:
            return []
        rows, hm = self._data_rows_with_header()
        out: list[dict] = []
        for i, vals in enumerate(rows, start=2):
            rd = self._row_dict(vals, i, hm)
            if q in (rd.get("account_nm") or "").lower():
                out.append(rd)
        return out

    def _is_pending(self, rd: dict) -> bool:
        v = (rd.get("_classification_cell") or "").strip().casefold()
        return v == "" or v.casefold() == CLASSIFICATION_PENDING.casefold()

    def find_next_pending(self) -> dict | None:
        rows, hm = self._data_rows_with_header()
        for i, vals in enumerate(rows, start=2):
            rd = self._row_dict(vals, i, hm)
            if rd.get("opp_id") and self._is_pending(rd):
                return rd
        return None

    def get_all_pending_rows(self) -> list[dict]:
        rows, hm = self._data_rows_with_header()
        out: list[dict] = []
        for i, vals in enumerate(rows, start=2):
            rd = self._row_dict(vals, i, hm)
            if rd.get("opp_id") and self._is_pending(rd):
                out.append(rd)
        return out

    def count_pending(self) -> int:
        return len(self.get_all_pending_rows())

    def write_classification(self, row_index: int, classification: str) -> None:
        write_classification_sheet_cell(self._worksheet(), row_index, classification)
