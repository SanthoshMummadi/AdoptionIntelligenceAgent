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


def extract_usd(value) -> float:
    """Extract USD amount from various formats."""
    if value is None:
        return 0
    try:
        return abs(float(value))
    except (TypeError, ValueError):
        return 0


def resolve_money(display: dict, opp: dict, field: str) -> float:
    """Resolve money field from display or opp."""
    # Try display first
    val = display.get(field, 0)
    if val and val != "N/A":
        try:
            return float(str(val).replace("$", "").replace(",", "").replace("M", "000000").replace("K", "000"))
        except Exception:
            pass

    # Try opp
    if field == "atr":
        return abs(opp.get("Amount", 0) or 0)
    elif field == "attrition":
        return abs(opp.get("Forecasted_Attrition__c", 0) or 0)

    return 0


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
    """Build Slack Block Kit UI for account brief."""
    account_name = account.get("name", "Unknown")
    account_id = account.get("id", "")

    # Build org62 link
    org62_link = f"https://org62.my.salesforce.com/{account_id}" if account_id else "#"

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Account Risk Briefing — <{org62_link}|{account_name}>*",
            },
        },
        {"type": "divider"},
    ]

    # TL;DR section
    if tldr:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*TL;DR*\n{tldr}",
            },
        })
        blocks.append({"type": "divider"})

    # Financials
    atr = fmt_amount(opp.get("Amount", 0) if opp else 0)
    forecasted_atr = fmt_amount(opp.get("Forecasted_Attrition__c", 0) if opp else 0)
    swing = fmt_amount(opp.get("Swing__c", 0) if opp else 0)
    close_date = opp.get("CloseDate", "N/A") if opp else "N/A"

    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*ATR:* {atr}"},
            {"type": "mrkdwn", "text": f"*Forecasted Attrition:* {forecasted_atr}"},
            {"type": "mrkdwn", "text": f"*Swing:* {swing}"},
            {"type": "mrkdwn", "text": f"*Close Date:* {close_date}"},
        ],
    })

    # ARI & Utilization
    ari_cat = snowflake_display.get("ari_category", "N/A")
    ari_prob = snowflake_display.get("ari_probability", "N/A")
    util = snowflake_display.get("utilization_rate", "N/A")

    ari_emoji = {
        "High": ":red_circle:",
        "Medium": ":large_yellow_circle:",
        "Low": ":large_green_circle:",
    }.get(ari_cat, ":white_circle:")

    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*ARI:* {ari_emoji} {ari_cat} ({ari_prob}%)"},
            {"type": "mrkdwn", "text": f"*Utilization:* {util}"},
        ],
    })

    blocks.append({"type": "divider"})

    # Risk Assessment
    if risk_notes:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Risk Assessment*\n{risk_notes}",
            },
        })

    # Recommendations
    if recommendation:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Recommended Actions*\n{recommendation}",
            },
        })

    # Product attrition breakdown
    product_attrition = account.get("product_attrition", [])
    if product_attrition:
        product_lines = []
        for p in product_attrition[:5]:
            product_lines.append(
                f"- {p.get('product', 'N/A')}: {fmt_amount(p.get('attrition', 0))}"
            )

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Product Attrition Breakdown*\n" + "\n".join(product_lines),
            },
        })

    # Red Account
    if red_account and not red_account.get("_historical"):
        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f":red_circle: *Red Account*\n"
                    f"- Stage: {red_account.get('Stage__c', 'N/A')}\n"
                    f"- ACV at Risk: {fmt_amount(red_account.get('ACV_at_Risk__c', 0))}\n"
                    f"- Days Red: {red_account.get('Days_Red__c', 0)}"
                ),
            },
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
