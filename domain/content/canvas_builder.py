"""
canvas_builder.py
Canvas builder for account briefs and GM Review canvases.
"""
import re
from datetime import datetime, timezone
from typing import Any, Dict

from log_utils import log_debug


def fmt_amount(value) -> str:
    """Format amount as $XM or $XK."""
    try:
        val = float(value)
        if val >= 1000000:
            return f"${val/1000000:.1f}M"
        elif val >= 1000:
            return f"${val/1000:.0f}K"
        else:
            return f"${val:,.0f}"
    except (TypeError, ValueError):
        return "N/A"


def _sanitize_cell(text: str) -> str:
    """Sanitize text for canvas table cells."""
    if not text:
        return ""

    text = str(text).strip()
    # Remove newlines
    text = text.replace("\n", " ").replace("\r", " ")
    # Remove pipe characters
    text = text.replace("|", "&#124;")
    # Truncate if too long
    if len(text) > 500:
        text = text[:497] + "..."

    return text


def get_canvas_url(canvas_id: str) -> str:
    """Build canvas URL."""
    team_id = "T2E6RHTM0"
    return f"https://salesforce.enterprise.slack.com/docs/{team_id}/{canvas_id}"


def create_canvas(client, title: str, markdown: str, user_id: str) -> str:
    """Create a Slack canvas and return URL."""
    try:
        response = client.api_call(
            "canvases.create",
            json={
                "title": title,
                "document_content": {
                    "type": "markdown",
                    "markdown": markdown,
                },
            },
        )

        if response.get("ok"):
            canvas_id = response.get("canvas_id")

            # Try to share with user
            try:
                client.api_call(
                    "canvases.access.set",
                    json={
                        "canvas_id": canvas_id,
                        "access_level": "write",
                        "user_ids": [user_id],
                    },
                )
            except Exception as e:
                log_debug(f"Canvas share warning: {e}")

            return get_canvas_url(canvas_id)
        else:
            raise Exception(response.get("error", "Unknown error"))

    except Exception as e:
        log_debug(f"Canvas creation error: {e}")
        return ""


def build_account_brief_blocks(
    account: dict,
    opp: dict,
    red_account: dict,
    snowflake_display: dict,
    risk_notes: str,
    recommendation: str,
    tldr: str = None,
) -> list:
    """Slack Block Kit: header, metrics, risk/actions, product, red account, TL;DR last, then SF link."""
    account_name = account.get("name", "Unknown")
    account_id = account.get("id", "")
    product_attrition = account.get("product_attrition", [])

    blocks: list = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"🚨 Account Risk Briefing — {account_name}",
                "emoji": True,
            },
        }
    ]

    if opp and opp.get("Name"):
        blocks.append({
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f":clipboard: *Renewal:* {opp.get('Name')} | "
                        f"Stage: {opp.get('StageName', 'N/A')} | "
                        f"Close: {opp.get('CloseDate', 'N/A')}"
                    ),
                }
            ],
        })

    blocks.append({"type": "divider"})

    ari_emoji = snowflake_display.get("ari_emoji", ":white_circle:")
    ari_category = snowflake_display.get("ari_category", "Unknown")
    ari_probability = snowflake_display.get("ari_probability", "N/A")
    health_display = snowflake_display.get("health_display", ":white_circle: Unknown")

    blocks.append({
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": (
                    f"*ARI Score:*\n{ari_emoji} *{ari_category} Risk* ({ari_probability})"
                ),
            },
            {
                "type": "mrkdwn",
                "text": f"*Account Health:*\n{health_display}",
            },
        ],
    })

    cc_aov = snowflake_display.get("cc_aov", "Unknown")
    util_emoji = snowflake_display.get("util_emoji", ":white_circle:")
    util_rate = snowflake_display.get("utilization_rate", "N/A")

    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*Cloud AOV:*\n{cc_aov}"},
            {"type": "mrkdwn", "text": f"*Utilization:*\n{util_emoji} {util_rate}"},
        ],
    })

    if opp:
        forecasted_atr = opp.get("Forecasted_Attrition__c")
        close_date = opp.get("CloseDate")
        if forecasted_atr is not None or close_date:
            try:
                fatr = abs(float(forecasted_atr or 0))
            except (TypeError, ValueError):
                fatr = 0.0
            blocks.append({
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Forecasted Attrition:*\n${fatr:,.0f}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Close Date:*\n{close_date or 'N/A'}",
                    },
                ],
            })

    blocks.append({"type": "divider"})

    if risk_notes:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*:mag: Risk Assessment*\n{risk_notes}",
            },
        })

    if recommendation:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*:dart: Recommended Actions*\n{recommendation}",
            },
        })

    if product_attrition:
        product_lines = []
        for p in product_attrition[:5]:
            product_name = (
                p.get("product")
                or p.get("APM_LVL_3")
                or p.get("APM_LVL_2")
                or p.get("APM_LVL_1")
                or "Unknown"
            )
            product_ari = (
                p.get("category")
                or p.get("ATTRITION_PROBA_CATEGORY")
                or "Unknown"
            )
            raw_reason = p.get("reason") or p.get("ATTRITION_REASON") or "N/A"
            product_reason = str(raw_reason)[:40]
            raw_atr = p.get("attrition")
            if raw_atr is not None:
                try:
                    atr_val = abs(float(raw_atr))
                except (TypeError, ValueError):
                    atr_val = 0.0
            else:
                try:
                    atr_val = abs(float(p.get("ATTRITION_PIPELINE") or 0))
                except (TypeError, ValueError):
                    atr_val = 0.0

            if product_ari == "High":
                emoji = ":red_circle:"
            elif product_ari == "Medium":
                emoji = ":large_yellow_circle:"
            elif product_ari == "Low":
                emoji = ":large_green_circle:"
            else:
                emoji = ":white_circle:"

            product_lines.append(
                f"{emoji} *{product_name}*: {product_ari} (${atr_val:,.0f}) — {product_reason}"
            )

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*:bar_chart: Product Breakdown:*\n" + "\n".join(product_lines),
            },
        })

    if red_account:
        stage = red_account.get("Stage__c", "Unknown")
        days_red = red_account.get("Days_Red__c", "N/A")
        latest = (red_account.get("Latest_Updates__c") or "")[:100]
        hist = " _(historical)_" if red_account.get("_historical") else ""
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*:red_circle: Red Account Status:*{hist}\n"
                    f"{stage} ({days_red} days)\n_{latest}_"
                ),
            },
        })

    blocks.append({"type": "divider"})

    if tldr:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*:bulb: TL;DR:*\n_{tldr}_",
            },
        })

    if account_id:
        sf_url = f"https://org62.my.salesforce.com/{account_id}"
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "🔗 View in Salesforce", "emoji": True},
                    "url": sf_url,
                    "style": "primary",
                }
            ],
        })

    return blocks


def build_canvas_row(
    opp: dict,
    account: dict,
    red_account: dict,
    snowflake_display: dict,
    risk_notes: str,
    recommendation: str,
    cloud: str = "Commerce Cloud",
) -> dict:
    """Build a single canvas row for GM Review."""
    account_name = account.get("name", "Unknown")
    account_id = account.get("id", "")

    # Build org62 link
    org62_link = f"https://org62.my.salesforce.com/{account_id}"
    account_link = f"[{account_name}]({org62_link})"

    # Extract data
    cc_aov = snowflake_display.get("cc_aov", "N/A")
    atr = fmt_amount(opp.get("Amount", 0) if opp else 0)
    for_attrition = fmt_amount(opp.get("Forecasted_Attrition__c", 0) if opp else 0)
    util_rate = snowflake_display.get("utilization_rate", "N/A")
    gmv_rate = snowflake_display.get("gmv_rate", "N/A")
    close_date = opp.get("CloseDate", "N/A") if opp else "N/A"
    territory = snowflake_display.get("territory", "N/A")

    # Red account flag
    red_ac_flag = "Yes" if (red_account and not red_account.get("_historical")) else "No"

    # Risk/notes
    risk_reason = opp.get("License_At_Risk_Reason__c", "") if opp else ""
    next_step = opp.get("NextStep", "") if opp else ""
    notes = f"{risk_reason}\n{next_step}".strip()

    return {
        "account_link": account_link,
        "account_id": account_id,
        "cc_aov": cc_aov,
        "atr": atr,
        "for_attrition": for_attrition,
        "util_rate": util_rate,
        "gmv_rate": gmv_rate,
        "close_date": close_date,
        "territory": territory,
        "red_ac_flag": red_ac_flag,
        "sf_products": cloud,
        "notes": _sanitize_cell(notes),
        "risk_details": _sanitize_cell(risk_notes),
        "recommendation": _sanitize_cell(recommendation),
    }


def build_gm_review_canvas(
    cloud: str,
    today: str,
    rows: list,
    region: str = None,
    fy: str = None,
    quarter: str = None,
) -> str:
    """Build full GM Review canvas markdown."""
    # Build filter label
    filter_parts = [cloud]
    if region:
        filter_parts.append(region)
    if quarter and fy:
        filter_parts.append(f"{quarter} {fy}")
    elif fy:
        filter_parts.append(fy)

    filter_label = " - ".join(filter_parts)

    # Calculate totals
    def parse_amt(val):
        try:
            v = str(val).replace("$", "").replace(",", "").strip()
            if v.endswith("M"):
                return float(v[:-1]) * 1000000
            elif v.endswith("K"):
                return float(v[:-1]) * 1000
            return float(v)
        except Exception:
            return 0

    total_atr = sum(parse_amt(r.get("for_attrition", 0)) for r in rows)
    total_aov = sum(parse_amt(r.get("cc_aov", 0)) for r in rows)

    # Build table
    table = (
        "|ACCOUNT|CC AOV|ATR|Forecasted Attrition|GMV Rate|Util Rate|"
        "Close Date|Territory|SF Products|Notes|Risk Analysis|Recommendation|\n"
        "|---|---|---|---|---|---|---|---|---|---|---|---|\n"
    )

    for row in rows:
        table += (
            f"|{row.get('account_link', '')}|"
            f"{_sanitize_cell(row.get('cc_aov', 'N/A'))}|"
            f"{_sanitize_cell(row.get('atr', 'N/A'))}|"
            f"{_sanitize_cell(row.get('for_attrition', 'N/A'))}|"
            f"{_sanitize_cell(row.get('gmv_rate', 'N/A'))}|"
            f"{_sanitize_cell(row.get('util_rate', 'N/A'))}|"
            f"{_sanitize_cell(row.get('close_date', 'N/A'))}|"
            f"{_sanitize_cell(row.get('territory', 'N/A'))}|"
            f"{_sanitize_cell(row.get('sf_products', 'N/A'))}|"
            f"{row.get('notes', '')}|"
            f"{row.get('risk_details', '')}|"
            f"{row.get('recommendation', '')}|\n"
        )

    # Build canvas
    canvas = f"""# {cloud} GM Review — {filter_label}
### {today}

---

# At-Risk Renewals

{table}

---

# Summary

|Metric|Value|
|---|---|
|Total Accounts|{len(rows)}|
|Total CC AOV|{fmt_amount(total_aov)}|
|Total Forecasted Attrition|{fmt_amount(total_atr)}|
|Generated|{today}|

_Data: Salesforce org62 · Snowflake ARI · Claude AI_
"""

    return canvas


class CanvasBuilder:
    """OO facade over canvas markdown helpers for adapters / tests."""

    def build_gm_review(self, account_data: Dict[str, Any]) -> str:
        """Build GM review markdown from a structured payload.

        **Table mode:** ``cloud``, ``today``, ``rows`` (list of table row dicts),
        optional ``region``, ``fy``, ``quarter``.

        **Workflow mode:** merged bundle with ``salesforce``, ``risk_analysis``,
        ``adoption_pov`` — renders a single-account narrative (no ``rows``).
        """
        today = account_data.get("today") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        rows = account_data.get("rows")
        if rows:
            return build_gm_review_canvas(
                cloud=account_data.get("cloud", "Commerce Cloud"),
                today=today,
                rows=rows,
                region=account_data.get("region"),
                fy=account_data.get("fy"),
                quarter=account_data.get("quarter"),
            )

        acc = account_data.get("salesforce", {}).get("account") or {}
        title = acc.get("Name") or account_data.get("account_id") or "Account"
        parts = [
            f"# GM Review — {_sanitize_cell(str(title))}",
            f"### {today}",
            "",
            "---",
            "",
        ]
        if account_data.get("risk_analysis"):
            parts.append(self.format_risk_section(account_data["risk_analysis"]))
            parts.append("")
        if account_data.get("adoption_pov"):
            parts.append(self.format_adoption_section(account_data["adoption_pov"]))
        return "\n".join(parts).rstrip() + "\n"

    def format_risk_section(self, risk_data: Dict[str, Any]) -> str:
        """Format risk analysis as markdown (brief + stats)."""
        parts = ["## Risk", ""]
        if risk_data.get("summary"):
            parts.append(str(risk_data["summary"]))
            parts.append("")
        if risk_data.get("risk_notes"):
            parts.append(str(risk_data["risk_notes"]))
            parts.append("")
        if risk_data.get("recommendation"):
            parts.append("**Recommended actions**")
            parts.append(str(risk_data["recommendation"]))
            parts.append("")
        cat = risk_data.get("ari_category") or risk_data.get("category")
        prob = risk_data.get("ari_probability") or risk_data.get("probability")
        if cat is not None or prob is not None:
            parts.append(f"**ARI:** {cat or 'N/A'} ({prob if prob is not None else 'N/A'})")
        if risk_data.get("license_at_risk_reason"):
            parts.append(f"**License at risk:** {_sanitize_cell(str(risk_data['license_at_risk_reason']))}")
        return "\n".join(p for p in parts if p is not None).rstrip() + "\n"

    def format_adoption_section(self, adoption_data: Dict[str, Any]) -> str:
        """Format adoption / utilization POV as markdown."""
        lines = ["## Adoption & usage", ""]
        util = adoption_data.get("utilization_rate")
        gmv = adoption_data.get("gmv_rate")
        burn = adoption_data.get("burn_rate")
        aov = adoption_data.get("cc_aov")
        terr = adoption_data.get("territory")
        geo = adoption_data.get("csg_geo")
        if util is not None:
            lines.append(f"- **Utilization:** {util}")
        if gmv is not None:
            lines.append(f"- **GMV rate:** {gmv}")
        if burn is not None:
            lines.append(f"- **Burn rate:** {burn}")
        if aov is not None:
            lines.append(f"- **CC AOV:** {aov}")
        if terr is not None:
            lines.append(f"- **Territory:** {terr}")
        if geo is not None:
            lines.append(f"- **CSG geo:** {geo}")
        if adoption_data.get("narrative"):
            lines.append("")
            lines.append(str(adoption_data["narrative"]))
        if len(lines) <= 2:
            lines.append("_No adoption metrics provided._")
        return "\n".join(lines) + "\n"
