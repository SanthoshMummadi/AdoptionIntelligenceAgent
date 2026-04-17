import os
"""
domain/content/canvas_builder.py
Canvas generation for Slack (account brief blocks + GM Review markdown).
"""
import os
import re
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from domain.analytics.snowflake_client import (
    cloud_aov_label,
    fmt_amount,
    get_sf_products_display,
    split_products_by_type,
)
from log_utils import log_debug


def _ari_emoji(category: str) -> str:
    if category == "High":
        return ":red_circle:"
    if category == "Medium":
        return ":large_yellow_circle:"
    if category == "Low":
        return ":large_green_circle:"
    return ":white_circle:"


def _health_emoji(score) -> str:
    try:
        s = float(score)
        if s >= 70:
            return ":large_green_circle:"
        if s >= 40:
            return ":large_yellow_circle:"
        return ":red_circle:"
    except (TypeError, ValueError):
        return ":white_circle:"


def _util_emoji(util_rate) -> str:
    try:
        val = float(str(util_rate).rstrip("%"))
        if val >= 70:
            return ":large_green_circle:"
        if val >= 40:
            return ":large_yellow_circle:"
        return ":red_circle:"
    except (TypeError, ValueError):
        return ":white_circle:"


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


def clean_html(text: str) -> str:
    """Strip HTML tags and normalize whitespace for Slack/canvas text."""
    if not text:
        return ""
    text = text.replace("<p>", "").replace("</p>", " ")
    text = text.replace("<br>", " ").replace("<br/>", " ")
    text = text.replace("<strong>", "**").replace("</strong>", "**")
    text = text.replace("<em>", "_").replace("</em>", "_")
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&#39;", "'").replace("&nbsp;", " ")
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def build_adoption_pov(usage_data: list, cloud: str = "Commerce Cloud") -> str:
    """
    Build Adoption POV from CIDM.WV_AV_USAGE_EXTRACT_VW data.

    Commerce: aggregate provisioned/used by L2.
    FSC: count orgs and calculate average utilization.
    """
    if not usage_data:
        return "No adoption data available."

    cloud_l1_map: Dict[str, List[str]] = {
        "Commerce Cloud": ["commerce"],
        "Financial Services Cloud": ["industries", "cross cloud - industries"],
    }
    exclude_l2 = ["success plan", "partner", "sandbox"]
    exclude_type = ["on demand"]
    l1_filters = cloud_l1_map.get(cloud, ["commerce"])

    filtered_rows = [
        u
        for u in usage_data
        if str(u.get("DRVD_APM_LVL_1") or "").strip().lower() in l1_filters
        and not any(x in str(u.get("DRVD_APM_LVL_2") or "").lower() for x in exclude_l2)
        and not any(x in str(u.get("TYPE") or "").lower() for x in exclude_type)
    ]

    if not filtered_rows:
        return "No adoption data available."

    # FSC branch: use only L2=Financial Services Cloud for utilization summary.
    if cloud == "Financial Services Cloud":
        fsc_rows = [
            r
            for r in filtered_rows
            if str(r.get("DRVD_APM_LVL_2") or "").strip().lower()
            == "financial services cloud"
            and float(r.get("PROVISIONED") or 0) > 0
        ]
        if not fsc_rows:
            return "No adoption data available."

        org_count = len({r.get("ACCOUNT_ID") for r in fsc_rows if r.get("ACCOUNT_ID")})

        product_agg: Dict[str, Dict[str, float]] = {}
        for r in fsc_rows:
            ptype = str(r.get("TYPE") or "Unknown").strip()
            provisioned = float(r.get("PROVISIONED") or 0)
            activated = float(r.get("ACTIVATED") or 0)
            used = float(r.get("USED") or 0)
            # LA first (activated/provisioned), LU fallback (used/provisioned).
            active = activated if activated > 0 else used
            if ptype not in product_agg:
                product_agg[ptype] = {"provisioned": 0.0, "active": 0.0}
            product_agg[ptype]["provisioned"] += provisioned
            product_agg[ptype]["active"] += active

        lines = [f"- Financial Services Cloud: {org_count} orgs"]
        for ptype, vals in sorted(
            product_agg.items(), key=lambda x: x[1]["provisioned"], reverse=True
        ):
            p = vals["provisioned"]
            a = vals["active"]
            util = f"{round(a / p * 100)}%" if p > 0 and a > 0 else "0%"
            lines.append(
                f"  - {ptype}: {p:,.0f} provisioned, {util} utilized ({a:,.0f} active)"
            )
        return "\n".join(lines)

    # Non-Commerce fallback: summarize unique ACCOUNT_ID orgs + average utilization.
    if cloud != "Commerce Cloud":
        org_count = len(
            {r.get("ACCOUNT_ID") for r in filtered_rows if r.get("ACCOUNT_ID")}
        )
        total_util = 0.0
        count = 0
        for r in filtered_rows:
            provisioned = float(r.get("PROVISIONED") or 0)
            activated = float(r.get("ACTIVATED") or 0)
            used = float(r.get("USED") or 0)
            active = activated if activated > 0 else used
            if provisioned > 0 and active > 0:
                total_util += (active / provisioned)
                count += 1
        avg_util = round((total_util / count) * 100) if count > 0 else 0
        return f"- {cloud}: {org_count} orgs, {avg_util}% average utilization"

    l2_summary: Dict[str, Dict[str, float]] = {}
    for u in filtered_rows:
        l2 = str(u.get("DRVD_APM_LVL_2") or "").strip()
        if not l2:
            continue
        provisioned = float(
            u.get("TOTAL_PROV") or u.get("PROVISIONED") or 0
        )
        activated = float(
            u.get("TOTAL_ACTIVATED") or u.get("ACTIVATED") or 0
        )
        used = float(u.get("TOTAL_USED") or u.get("USED") or 0)
        active = activated if activated > 0 else used

        if l2 not in l2_summary:
            l2_summary[l2] = {"provisioned": 0.0, "active": 0.0}
        l2_summary[l2]["provisioned"] += provisioned
        l2_summary[l2]["active"] += active

    pov_lines: list[str] = []
    for l2, vals in sorted(
        l2_summary.items(),
        key=lambda x: x[1]["provisioned"],
        reverse=True,
    ):
        prov = vals["provisioned"]
        active = vals["active"]
        if prov <= 0:
            continue
        pct = (active / prov) * 100

        if prov >= 1_000_000:
            prov_fmt = f"{prov / 1_000_000:.2f}M"
            active_fmt = f"{active / 1_000_000:.2f}M"
        else:
            prov_fmt = f"{prov:,.0f}"
            active_fmt = f"{active:,.0f}"

        pov_lines.append(
            f"- {l2}: {prov_fmt} provisioned, "
            f"{pct:.0f}% utilized ({active_fmt} used)"
        )

    return "\n".join(pov_lines) if pov_lines else "No adoption data available."


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
    user_cloud: str = "",
    all_products: list | None = None,
) -> list:
    """Slack Block Kit: header, ARI/health, AOV/util, financial status, products, notes, risk, actions, summary, link."""
    account_name = account.get("name", "Unknown")
    account_id = account.get("id", "")
    product_attrition = account.get("product_attrition", [])

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Account Risk Briefing — {account_name}",
                "emoji": True,
            },
        }
    ]

    ari_emoji = snowflake_display.get("ari_emoji", ":white_circle:")
    ari_category = snowflake_display.get("ari_category", "Unknown")
    ari_prob = snowflake_display.get("ari_probability", "N/A")

    health_score = snowflake_display.get("health_score", "")
    health_emoji = _health_emoji(health_score)

    blocks.append({
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": f"*ARI Score:*\n{ari_emoji} *{ari_category} Risk* ({ari_prob})",
            },
            {
                "type": "mrkdwn",
                "text": f"*Account Health:*\n{health_emoji} {health_score}",
            },
        ],
    })

    cc_aov = snowflake_display.get("cc_aov", "Unknown")
    util_rate = snowflake_display.get("utilization_rate", "N/A")

    c_lbl = (user_cloud or "").strip() or "Commerce Cloud"
    aov_lbl = cloud_aov_label(c_lbl)

    if util_rate and util_rate != "N/A":
        util_emoji = _util_emoji(util_rate)
        util_display = f"{util_emoji} {util_rate}"
    else:
        util_display = ":white_circle: N/A"

    blocks.append({
        "type": "section",
        "fields": [
            {"type": "mrkdwn", "text": f"*{aov_lbl}:*\n{cc_aov}"},
            {"type": "mrkdwn", "text": f"*Utilization:*\n{util_display}"},
        ],
    })

    blocks.append({"type": "divider"})

    blocks.append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": "*:bar_chart: Financial & Status*"},
    })

    fin_fields = []
    if opp and opp.get("Name"):
        fin_fields.append({
            "type": "mrkdwn",
            "text": f"*Renewal:*\n{opp.get('Name', 'N/A')}",
        })
    if opp and opp.get("CloseDate"):
        fin_fields.append({
            "type": "mrkdwn",
            "text": f"*Close Date:*\n:date: {opp.get('CloseDate')}",
        })
    if fin_fields:
        blocks.append({"type": "section", "fields": fin_fields})

    try:
        atr_org62 = (
            abs(float(opp.get("Forecasted_Attrition__c", 0) or 0)) if opp else 0.0
        )
    except (TypeError, ValueError):
        atr_org62 = 0.0
    try:
        atr_snow = float(snowflake_display.get("renewal_atr", 0) or 0)
    except (TypeError, ValueError):
        atr_snow = 0.0
    atr_shown = atr_org62 or atr_snow
    try:
        swing = abs(float(opp.get("Swing__c", 0) or 0)) if opp else 0.0
    except (TypeError, ValueError):
        swing = 0.0
    forecast_judgement = opp.get("Manager_Forecast_Judgement__c", "") if opp else ""

    atr_fields = []
    if atr_shown > 0:
        atr_fields.append({
            "type": "mrkdwn",
            "text": f"*ATR:*\n:chart_with_downwards_trend: {fmt_amount(atr_shown)}",
        })
    if atr_fields:
        blocks.append({"type": "section", "fields": atr_fields})

    swing_fields = []
    if swing > 0:
        swing_fields.append({
            "type": "mrkdwn",
            "text": f"*Swing:*\n:arrows_counterclockwise: {fmt_amount(swing)}",
        })
    if forecast_judgement:
        swing_fields.append({
            "type": "mrkdwn",
            "text": f"*Forecast:*\n:bar_chart: {forecast_judgement}",
        })
    if swing_fields:
        blocks.append({"type": "section", "fields": swing_fields})

    risk_reason = opp.get("License_At_Risk_Reason__c", "") if opp else ""
    if risk_reason:
        blocks.append({
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Risk Reason:*\n:dart: {risk_reason}"},
            ],
        })

    blocks.append({"type": "divider"})

    if product_attrition:
        split = split_products_by_type(product_attrition)
        all_products = split["core"] + split["success_plans"]

        product_lines = []
        for p in all_products[:8]:
            product_name = (
                p.get("APM_LVL_3")
                or p.get("APM_LVL_2")
                or p.get("APM_LVL_1")
                or p.get("product")
                or "Unknown"
            )
            product_ari = (
                p.get("ATTRITION_PROBA_CATEGORY")
                or p.get("category")
                or "Unknown"
            )
            try:
                pipe = p.get("ATTRITION_PIPELINE")
                if pipe is not None:
                    atr_val = abs(float(pipe or 0))
                else:
                    atr_val = abs(float(p.get("attrition", 0) or 0))
            except (TypeError, ValueError):
                atr_val = 0.0

            emoji = _ari_emoji(product_ari)
            aov_str = f"AOV: {fmt_amount(atr_val)}" if atr_val > 0 else ""
            line = f"{emoji} *{product_name}* | {product_ari}"
            if aov_str:
                line += f" | {aov_str}"
            product_lines.append(line)

        if product_lines:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*:package: Product Attrition Breakdown*\n" + "\n".join(product_lines),
                },
            })

    blocks.append({"type": "divider"})

    manager_notes_parts = []
    if opp:
        forecast_j = opp.get("Manager_Forecast_Judgement__c", "")
        specialist_notes = opp.get("Specialist_Sales_Notes__c", "")
        description = opp.get("Description", "")
        next_step = opp.get("NextStep", "")

        if forecast_j:
            manager_notes_parts.append(f"*Forecast Judgement:* {forecast_j}")
        if specialist_notes:
            manager_notes_parts.append(f"*CSG Notes:* {specialist_notes[:200]}")
        elif description:
            manager_notes_parts.append(f"*Notes:* {description[:200]}")
        if next_step:
            manager_notes_parts.append(f"*Next Step:* {next_step[:150]}")

    if red_account:
        stage = red_account.get("Stage__c", "")
        days_red = red_account.get("days_red")
        if days_red is None:
            days_red = red_account.get("Days_Red__c") or 0
        try:
            days_red = int(days_red) if days_red is not None else 0
        except (TypeError, ValueError):
            days_red = 0
        days_red_str = f"{days_red} days" if days_red > 0 else "N/A"
        latest = (red_account.get("Latest_Updates__c") or "")[:150]

        if stage:
            manager_notes_parts.append(
                f"*Red Account:* :red_circle: {stage} ({days_red_str})"
            )
        if latest:
            manager_notes_parts.append(f"*Latest Update:* {clean_html(latest)}")

    if manager_notes_parts:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*:pushpin: Manager Notes*\n" + "\n".join(manager_notes_parts),
            },
        })
        blocks.append({"type": "divider"})

    if risk_notes:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*:mag: Risk Assessment*\n{risk_notes}"},
        })

    if recommendation:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*:dart: Recommended Actions*\n{recommendation}"},
        })

    blocks.append({"type": "divider"})

    if tldr:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*:bulb: Executive Summary*\n_{tldr}_",
            },
        })

    if account_id:
        sf_url = f"https://org62.my.salesforce.com/{account_id}"
        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "🔗 View in Salesforce",
                        "emoji": True,
                    },
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
    territory = (
        snowflake_display.get("territory")
        or snowflake_display.get("csg_territory")
        or "N/A"
    )

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


def build_review_row(
    opp: dict,
    account: dict,
    red_account: dict,
    snowflake_display: dict,
    risk_notes: str,
    recommendation: str,
    cloud: str = "Commerce Cloud",
) -> dict:
    """Alias for list/table outputs (keeps data-only row shape)."""
    return build_canvas_row(
        opp=opp,
        account=account,
        red_account=red_account,
        snowflake_display=snowflake_display,
        risk_notes=risk_notes,
        recommendation=recommendation,
        cloud=cloud,
    )


def build_gm_review_canvas_markdown(
    reviews: list,
    cloud: str = "Commerce Cloud",
    filter_label: str = "",
    today: Optional[str] = None,
) -> str:
    """
    GM Review: single wide At-Risk Renewals table (13 columns), links, summary.
    """
    if not today:
        today = date.today().strftime("%A, %B %d, %Y")

    if not filter_label:
        filter_label = f"{cloud} - Q2 FY2027"

    if not reviews:
        return "\n".join(
            [
                f"# {cloud} — GM Review",
                f"### {today}",
                "",
                "_No accounts._",
            ]
        )

    c_lbl = (cloud or "").strip() or "Commerce Cloud"
    aov_hdr = cloud_aov_label(c_lbl)

    lines = [
        f"# {cloud} — GM Review",
        f"### {today}",
        "",
        "---",
        "",
        "## At-Risk Renewals",
        "",
        f"| ACCOUNT | ARI | {aov_hdr} | ATR | For. Attrition | GMV Rate | Util Rate | Close Date | Territory | SF Products | Notes | Risk Analysis | Recommendation |",
        "|---------|-----|--------|-----|----------------|----------|-----------|------------|-----------|-------------|-------|---------------|----------------|",
    ]

    total_aov = 0.0
    total_atr = 0.0

    for review in reviews:
        account_name = review.get("account_name", "Unknown")
        account_id = review.get("account_id", "")
        opp = review.get("opp") or {}
        display = review.get("snowflake_display") or {}
        red = review.get("red_account")
        enrichment = review.get("enrichment") or {}
        product_attrition = review.get("product_attrition") or []
        risk_notes = review.get("risk_notes", "")
        recommendation = review.get("recommendation", "")

        sf_url = f"https://org62.my.salesforce.com/{account_id}"
        account_link = f"[{account_name}]({sf_url})"

        ari_cat = display.get("ari_category", "Unknown")
        ari_prob = display.get("ari_probability", "N/A")
        if str(os.getenv("SNOWFLAKE_CSS_SKIP") or "").strip() in ("1", "true", "yes", "on"):
            ari_cell = "N/A"
        else:
            ari_cell = f"{ari_cat} ({ari_prob})"

        renewal_aov = float(
            enrichment.get("renewal_aov", {}).get("renewal_aov", 0) or 0
        )
        cc_aov_cell = f"${renewal_aov:,.0f}" if renewal_aov > 0 else "Unknown"
        total_aov += renewal_aov

        ren_blk = enrichment.get("renewal_aov", {}) or {}
        atr_snow = float(
            ren_blk.get("renewal_atr_snow", 0) or ren_blk.get("renewal_atr", 0) or 0
        )
        forecasted_atr = abs(float(opp.get("Forecasted_Attrition__c", 0) or 0))
        atr_merged = forecasted_atr or atr_snow
        atr_cell = fmt_amount(atr_merged) if atr_merged > 0 else "N/A"

        for_atr_cell = f"$-{forecasted_atr:,.0f}" if forecasted_atr > 0 else "N/A"
        total_atr += forecasted_atr or atr_snow

        gmv_rate = display.get("gmv_rate", "Unknown")
        util_rate = display.get("utilization_rate", "N/A")
        close_date = opp.get("CloseDate", "N/A")

        territory = (
            display.get("territory")
            or display.get("csg_territory")
            or str(enrichment.get("renewal_aov", {}).get("csg_territory", "") or "").strip()
            or str(enrichment.get("renewal_aov", {}).get("csg_geo", "") or "").strip()
            or (str(red.get("CSG_GEO__c", "") or "").strip() if red else "")
            or "N/A"
        )

        all_prods = review.get("all_products_attrition") or []
        sf_products = get_sf_products_display(all_prods)

        notes_parts = []
        specialist = (opp.get("Specialist_Sales_Notes__c") or "") if opp else ""
        description = (opp.get("Description") or "") if opp else ""
        notes_text = specialist or description
        if notes_text:
            notes_parts.append(notes_text)

        if red:
            latest = red.get("Latest_Updates__c") or ""
            if latest:
                notes_parts.append(f"Red Account: {clean_html(latest)}")

        notes_cell = " · ".join(notes_parts) if notes_parts else "N/A"
        notes_cell = notes_cell.replace("|", "-")

        if risk_notes:
            bullets = [
                line.strip().lstrip("- ").strip()
                for line in risk_notes.split("\n")
                if line.strip().startswith("-")
            ][:2]
            risk_cell = (
                " · - ".join(b[:80] for b in bullets)
                if bullets
                else risk_notes[:120]
            )
        else:
            risk_cell = "N/A"
        risk_cell = risk_cell.replace("|", "-")

        if recommendation:
            bullets = [
                line.strip().lstrip("- ").strip()
                for line in recommendation.split("\n")
                if line.strip().startswith("-")
            ][:2]
            rec_cell = (
                " · - ".join(b[:80] for b in bullets)
                if bullets
                else recommendation[:120]
            )
        else:
            rec_cell = "N/A"
        rec_cell = rec_cell.replace("|", "-")

        lines.append(
            f"| {account_link} | {ari_cell} | {cc_aov_cell} | {atr_cell} "
            f"| {for_atr_cell} | {gmv_rate} | {util_rate} | {close_date} "
            f"| {territory} | {sf_products} | {notes_cell} "
            f"| {risk_cell} | {rec_cell} |"
        )

    lines += ["", "---", ""]

    lines += [
        "## Account Links",
        "",
    ]
    for review in reviews:
        an = review.get("account_name", "Unknown")
        aid = review.get("account_id", "")
        url = f"https://org62.my.salesforce.com/{aid}"
        lines.append(f"- [{an}]({url})")

    lines += ["", "---", ""]

    lines += [
        "## Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| **Cloud** | {cloud} |",
        f"| **Filter** | {filter_label} |",
        f"| **Accounts Reviewed** | {len(reviews)} |",
        f"| **Total {aov_hdr}** | {fmt_amount(total_aov)} |",
        f"| **Total Forecasted Attrition** | ${total_atr:,.0f} |",
        f"| **Generated** | {today} |",
        "",
        "_Data: Salesforce org62 · Snowflake ARI · GMV Sheet · Claude AI_",
    ]

    return "\n".join(lines)


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

    c_lbl = (cloud or "").strip() or "Commerce Cloud"
    aov_hdr = cloud_aov_label(c_lbl)

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
        f"|ACCOUNT|{aov_hdr}|ATR|Forecasted Attrition|GMV Rate|Util Rate|"
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
|Total {aov_hdr}|{fmt_amount(total_aov)}|
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
        title = (
            account_data.get("account_name")
            or acc.get("Name")
            or account_data.get("account_id")
            or "Account"
        )
        title = " ".join(str(title).split())
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
        ap = account_data.get("adoption_pov")
        if ap:
            if isinstance(ap, dict):
                ap_m = dict(ap)
                wc = account_data.get("cloud")
                if wc:
                    ap_m.setdefault("cloud", wc)
                parts.append(self.format_adoption_section(ap_m))
            else:
                parts.append("## Adoption & usage\n\n" + str(ap) + "\n")
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
            c_ad = (adoption_data.get("cloud") or "").strip() or "Commerce Cloud"
            aov_l = cloud_aov_label(c_ad)
            lines.append(f"- **{aov_l}:** {aov}")
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
