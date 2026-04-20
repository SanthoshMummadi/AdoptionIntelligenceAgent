"""
App Home dashboard for Adoption Intelligence Bot.
Renders at-risk renewals, red accounts and recent GM Review sheets.
"""
import time

from slack_sdk import WebClient

from domain.analytics.bulk_renewals import get_atrisk_renewals_bulk
from domain.analytics.snowflake_client import fmt_amount
from log_utils import log_debug

_cache: dict = {}
_TTL = 3600  # 1 hour


def _get_renewals(cloud: str) -> list:
    key = cloud.lower()
    cached = _cache.get(key)
    if cached and (time.time() - cached["ts"] < _TTL):
        return cached["rows"]
    rows = get_atrisk_renewals_bulk(cloud, limit=500)
    _cache[key] = {"rows": rows, "ts": time.time()}
    return rows


def build_app_home(user_id: str) -> list:
    """Build App Home blocks for a user."""
    del user_id
    blocks = []
    commerce_rows: list[dict] = []
    fsc_rows: list[dict] = []

    # Header
    blocks += [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Adoption Intelligence Bot",
                "emoji": True,
            },
        },
        {"type": "divider"},
    ]

    # At-Risk Renewals Summary
    try:
        commerce_rows = _get_renewals("Commerce Cloud")
        fsc_rows = _get_renewals("Financial Services Cloud")
        commerce_count = len(commerce_rows)
        fsc_count = len(fsc_rows)
    except Exception as e:
        log_debug(f"App Home renewal fetch error: {e}")
        commerce_count = 0
        fsc_count = 0

    blocks += [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*At-Risk Renewals (FY27/28)*"},
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Commerce Cloud*\n{commerce_count} accounts at risk",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Financial Services Cloud*\n{fsc_count} accounts at risk",
                },
            ],
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Commerce GM Review", "emoji": True},
                    "style": "primary",
                    "action_id": "run_gm_review_commerce",
                    "value": "Commerce Cloud",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "FSC GM Review", "emoji": True},
                    "action_id": "run_gm_review_fsc",
                    "value": "Financial Services Cloud",
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Refresh", "emoji": True},
                    "action_id": "refresh_app_home",
                    "value": "refresh",
                },
            ],
        },
        {"type": "divider"},
    ]

    # Top Red Accounts
    blocks += [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Top Red Accounts Needing Attention*"},
        }
    ]

    all_rows = sorted(
        [r for r in (commerce_rows + fsc_rows) if r.get("days_red") or r.get("red_notes")],
        key=lambda x: abs(float(x.get("atr") or 0)),
        reverse=True,
    )[:5]

    if all_rows:
        for r in all_rows:
            atr = fmt_amount(abs(float(r.get("atr") or 0)))
            close = str(r.get("close_date", ""))[:7]
            territory = r.get("territory", "")
            account = r.get("account_name") or r.get("account") or "Unknown"
            opp_id = r.get("opp_id_18") or r.get("opportunity_id") or ""
            sf_url = f"https://org62.my.salesforce.com/{opp_id}" if opp_id else ""

            section_block = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{account}*\n"
                        f"ATR: {atr} | Closes: {close} | Territory: {territory}"
                    ),
                },
            }
            if sf_url:
                section_block["accessory"] = {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View in SF", "emoji": True},
                    "url": sf_url,
                }
            blocks.append(section_block)
    else:
        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": "_No red accounts found_"}}
        )

    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Use `/gm-review-sheet Commerce Cloud` or `/gm-review-sheet FSC` to export to Google Sheets",
                }
            ],
        }
    )
    return blocks


def publish_app_home(client: WebClient, user_id: str):
    """Publish App Home view for a user."""
    try:
        blocks = build_app_home(user_id)
        client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})
        log_debug(f"App Home published for {user_id}")
    except Exception as e:
        log_debug(f"App Home publish error: {e}")

