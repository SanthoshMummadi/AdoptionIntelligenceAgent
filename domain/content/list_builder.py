"""
domain/content/list_builder.py
Slack Lists helpers for GM Review.

Goal: Build record payloads using real column IDs/options from the list schema,
so we do not hardcode Col*/Opt* identifiers in code.
"""

from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
from slack_sdk import WebClient

from domain.analytics.snowflake_client import get_sf_products_display
from log_utils import log_debug


GM_REVIEW_LIST_SCHEMA = {
    "name": "Account Name",
    "Col07QP8JF3TP": "Cloud Aov",
    "Col07QZSBCSHY": "GMV Rate",
    "Col07QS1U2Z2P": "ATR",
    "Col07QS1YLF5Z": "Forcasted Attrition",
    "Col07R2BS8LUC": "Util. Rate",
    "Col0ATQFMLG2D": "Renewal Close Date",
    "Col0ATF6SNAE8": "CSG Territory",
    "Col0ATBJT8D5G": "SF Products",
    "Col0AU5SX0K0Q": "Risk Details/ Current Scenario",
    "Col0ASW5NAVST": "Recommendataions",
}


def _norm_title(s: str) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _extract_columns(list_schema: dict) -> list[dict[str, Any]]:
    """
    Slack list payloads vary by API version/workspace.
    Try common shapes: list["fields"], list["columns"], list["schema"]["fields"].
    """
    lst = list_schema.get("list") if isinstance(list_schema.get("list"), dict) else list_schema
    for key_path in (
        ("fields",),
        ("columns",),
        ("schema", "fields"),
        ("schema", "columns"),
    ):
        cur: Any = lst
        ok = True
        for k in key_path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and isinstance(cur, list):
            return [c for c in cur if isinstance(c, dict)]
    return []


def _column_id(col: dict) -> str:
    return (
        col.get("id")
        or col.get("field_id")
        or col.get("column_id")
        or col.get("key")
        or ""
    )


def _column_title(col: dict) -> str:
    return col.get("title") or col.get("name") or col.get("label") or ""


def _column_type(col: dict) -> str:
    t = col.get("type") or col.get("field_type") or ""
    return str(t)


def _extract_select_options(col: dict) -> dict[str, str]:
    """
    Returns mapping label -> option_id for select-like columns.
    """
    opts = col.get("options") or col.get("select_options") or []
    out: dict[str, str] = {}
    if isinstance(opts, list):
        for o in opts:
            if not isinstance(o, dict):
                continue
            label = str(o.get("label") or o.get("name") or "").strip()
            oid = str(o.get("id") or o.get("option_id") or "").strip()
            if label and oid:
                out[label.lower()] = oid
    return out


def build_list_record(row: dict, list_schema: dict = None) -> list[dict[str, str]]:
    """Build Slack List record payload from the workflow review row."""
    del list_schema

    opp = row.get("opp") or {}
    sf = row.get("snowflake_display") or {}
    enrichment = row.get("enrichment") or {}
    usage = enrichment.get("usage") or {}

    # Account Name with hyperlink to Opportunity
    opp_id = row.get("opportunity_id", "")
    account = row.get("account_name") or row.get("account", "Unknown")
    if opp_id:
        account_value = f"[{account}](https://org62.my.salesforce.com/{opp_id})"
    else:
        account_value = account

    # Cloud AOV — from snowflake_display or usage
    cc_aov = sf.get("cc_aov") or usage.get("cloud_aov") or row.get("cc_aov") or "N/A"

    # GMV Rate
    gmv_rate = (
        sf.get("gmv_rate")
        or usage.get("gmv_util")
        or row.get("gmv_rate")
        or "N/A"
    )

    # ATR — from opp.Amount
    atr_raw = (
        opp.get("Amount")
        or opp.get("Forecasted_Attrition__c")
        or row.get("atr")
        or 0
    )
    try:
        atr = f"${abs(float(atr_raw)):,.0f}" if atr_raw else "N/A"
    except (TypeError, ValueError):
        atr = str(atr_raw) if atr_raw else "N/A"

    # Forecasted Attrition
    fc_raw = opp.get("Forecasted_Attrition__c") or row.get("forecasted_attrition") or 0
    try:
        forecasted = f"${abs(float(fc_raw)):,.0f}" if fc_raw else "N/A"
    except (TypeError, ValueError):
        forecasted = str(fc_raw) if fc_raw else "N/A"

    # Util Rate
    util = (
        sf.get("utilization_rate")
        or usage.get("utilization_rate")
        or row.get("utilization_rate")
        or row.get("util_rate")
        or "N/A"
    )

    # Close Date
    close_date = opp.get("CloseDate") or row.get("close_date", "N/A")

    # Territory
    renewal_aov = enrichment.get("renewal_aov") or {}
    territory = (
        sf.get("csg_territory")
        or sf.get("territory")
        or enrichment.get("csg_territory")
        or renewal_aov.get("csg_territory")
        or row.get("territory")
        or opp.get("Account", {}).get("BillingCountry", "N/A")
    )

    # SF Products — use attrition product rows when available
    all_products = row.get("all_products_attrition") or []
    sf_products = get_sf_products_display(all_products)

    # Fallback to opp name if no product data
    if sf_products == "N/A":
        opp_name = opp.get("Name", "")
        if "B2B" in opp_name:
            sf_products = "B2B Commerce"
        elif "B2C" in opp_name or "Commerce" in opp_name:
            sf_products = "B2C Commerce"
        elif "Marketing" in opp_name:
            sf_products = "Marketing Cloud"
        else:
            sf_products = row.get("cloud", "Commerce Cloud")

    # Risk + Recommendation
    risk = row.get("risk_notes", "") or row.get("risk_detail", "")
    recommendation = row.get("recommendation", "")

    return [
        {"key": "name", "value": str(account_value)[:500]},
        {"key": "Col07QP8JF3TP", "value": str(cc_aov)[:200]},
        {"key": "Col07QZSBCSHY", "value": str(gmv_rate)[:100]},
        {"key": "Col07QS1U2Z2P", "value": str(atr)[:100]},
        {"key": "Col07QS1YLF5Z", "value": str(forecasted)[:100]},
        {"key": "Col07R2BS8LUC", "value": str(util)[:100]},
        {"key": "Col0ATQFMLG2D", "value": str(close_date)[:100]},
        {"key": "Col0ATF6SNAE8", "value": str(territory)[:200]},
        {"key": "Col0ATBJT8D5G", "value": str(sf_products)[:200]},
        {"key": "Col0AU5SX0K0Q", "value": str(risk)[:500]},
        {"key": "Col0ASW5NAVST", "value": str(recommendation)[:500]},
    ]


def update_slack_list(client, list_id: str, rows: list[dict]) -> dict:
    """
    Clear existing records and repopulate the Slack List.
    Returns {"updated": N, "errors": [..]}.
    """
    load_dotenv()

    del client
    errors: list[str] = []
    inserted = 0

    user_token = os.environ.get("SLACK_USER_TOKEN")
    if not user_token:
        return {"updated": 0, "errors": ["SLACK_USER_TOKEN not set"]}
    user_client = WebClient(token=user_token)

    # Use bot token for lists.records.create/delete (write)
    bot_token = os.environ.get("SLACK_BOT_TOKEN")
    bot_client = WebClient(token=bot_token) if bot_token else user_client

    # 1. Get existing records via files.info
    try:
        resp = user_client.files_info(file=list_id)
        existing = (
            resp.get("file", {})
            .get("list_metadata", {})
            .get("rows", [])
        )
    except Exception as e:
        existing = []
        errors.append(f"files.info failed: {e!r}")

    # Delete existing
    for rec in existing:
        rid = rec.get("record_id") or rec.get("id")
        if not rid:
            continue
        try:
            bot_client.api_call(
                "lists.records.delete",
                json={"list_id": list_id, "record_id": rid},
            )
        except Exception as e:
            errors.append(f"delete {rid}: {e!r}")

    # Insert new
    for row in rows:
        try:
            fields = build_list_record(row)
            bot_client.api_call(
                "lists.records.create",
                json={"list_id": list_id, "fields": fields},
            )
            inserted += 1
        except Exception as e:
            acct = row.get("account") or "?"
            errors.append(f"insert {acct}: {e!r}")

    print(f"update_slack_list: list_id={list_id}, updated={inserted}, errors={len(errors)}")
    if errors:
        for err in errors:
            print(f"  Error: {err}")
    return {"updated": inserted, "errors": errors}

