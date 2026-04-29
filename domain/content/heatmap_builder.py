"""
domain/content/heatmap_builder.py

Pure string formatting for Adoption Heatmap canvases.
No Snowflake calls. No slack_sdk imports.
Standard library only.
"""

from datetime import datetime
from zoneinfo import ZoneInfo
from domain.analytics.adoption_scoring import score_to_emoji
from domain.analytics.heatmap_queries import resolve_cloud
from domain.analytics.threshold_config import get_thresholds

_MAX_HEATMAP_CHARS = 3000
_MAX_DRILLDOWN_CHARS = 1500
_PRODUCT_HEADER_LEN = 20
_IST = ZoneInfo("Asia/Kolkata")


def _now_ist() -> str:
    now = datetime.now(tz=_IST)
    return now.strftime("%b %d %Y %H:%M IST")


def _truncate(text: str, length: int) -> str:
    if len(text) <= length:
        return text
    return text[:length - 1] + "…"


def _safe_cell(text: str, length: int) -> str:
    """Truncate and remove pipe chars that break markdown tables."""
    clean = str(text).replace("|", "/").replace("\n", " ")
    return _truncate(clean, length)


def _feature_link(feature: dict) -> str:
    """Returns Slack hyperlink if GUS URL available, else plain name."""
    if feature.get("gus_url"):
        return f"<{feature['gus_url']}|{feature['feature']}>"
    return feature.get("feature", "")


def _trend_arrow(trend) -> str:
    if trend is None:
        return "—"
    if trend >= 0:
        return f"↑ +{trend:.1f}%"
    return f"↓ {trend:.1f}%"


def _get_product_cap(quarter_count: int) -> int:
    """
    More products when fewer quarters (single quarter = more vertical space).
    - 1 quarter  → show up to 20 products
    - 2 quarters → show up to 12 products
    - 3 quarters → show up to 8 products
    - 4 quarters → show up to 6 products
    """
    return {1: 20, 2: 12, 3: 8}.get(quarter_count, 6)


def _narrative(product_rows: list) -> str:
    scores = [r["score"] for r in product_rows if r.get("score") is not None]
    trends = [r["trend"] for r in product_rows if r.get("trend") is not None]

    if not scores:
        return "No adoption data available for this product."

    avg_score = sum(scores) / len(scores)

    if trends:
        avg_trend = sum(trends) / len(trends)
        if avg_trend >= 10:
            direction = "growing"
        elif avg_trend <= -10:
            direction = "declining"
        else:
            direction = "flat"
    else:
        direction = "flat"

    if direction == "growing" and avg_score >= 70:
        return "Adoption is growing steadily across all quarters."
    elif direction == "growing" and avg_score >= 40:
        return "Usage is trending up — adoption approaching healthy threshold."
    elif direction == "declining" and avg_score < 40:
        return "Usage declined significantly — this product needs immediate attention."
    elif direction == "declining":
        return "Usage declined in recent quarters — watch this product closely."
    elif avg_score >= 70:
        return "Flat adoption — stable and above threshold, but no growth signal."
    elif avg_score >= 40:
        return "Flat adoption — hovering near the watch threshold, monitor closely."
    else:
        return "Flat adoption — consistently below threshold, intervention recommended."


def build_adoption_heatmap_canvas(
    scored_data: list,
    cloud: str,
    fy: str,
    industry: str | None = None,
    region: str | None = None,
) -> str:
    """
    Takes scored heatmap data and returns Slack Canvas markdown string.

    Input: list of feature dicts from get_adoption_heatmap_data()
    Each dict must have: feature, feature_group, quarter, status,
    score, account_count, mau, trend
    """

    if not scored_data:
        return (
            f"# {cloud} · Adoption Heatmap · {fy}\n\n"
            "_No adoption data available for this cloud and fiscal year._"
        )

    # Organise data
    all_quarters = sorted(
        {r["quarter"] for r in scored_data},
        key=lambda q: q[-2:]  # sort by Q1/Q2/Q3/Q4
    )
    all_products = sorted({r["feature"].replace("|", "/") for r in scored_data})

    # Build lookup: (feature, quarter) → row
    lookup = {
        (r["feature"].replace("|", "/"), r["quarter"]): r
        for r in scored_data
    }

    # Summary counts
    green_n = sum(1 for r in scored_data if r.get("status") == "green")
    amber_n = sum(1 for r in scored_data if r.get("status") == "amber")
    red_n = sum(1 for r in scored_data if r.get("status") == "red")
    total_accounts = max(
        (r.get("account_count", 0) for r in scored_data), default=0
    )
    total_products = len(all_products)

    # Truncate if needed
    quarter_count = len(all_quarters)
    max_products = _get_product_cap(quarter_count)
    truncated = False
    display_products = all_products
    if len(all_products) > max_products:
        def _max_acct(p):
            return max(
                (lookup.get((p, q), {}).get("account_count", 0)
                 for q in all_quarters),
                default=0
            )

        display_products = sorted(
            all_products, key=_max_acct, reverse=True
        )[:max_products]
        truncated = True

    # Top/bottom by avg score
    def _avg_score(p):
        scores = [
            lookup[(p, q)]["score"]
            for q in all_quarters
            if (p, q) in lookup
        ]
        return sum(scores) / len(scores) if scores else 0.0

    sorted_by_score = sorted(display_products, key=_avg_score, reverse=True)
    top_product = sorted_by_score[0] if sorted_by_score else "—"
    bottom_product = sorted_by_score[-1] if sorted_by_score else "—"
    top_score = round(_avg_score(top_product))
    bottom_score = round(_avg_score(bottom_product))

    filter_parts_c: list[str] = []
    if industry:
        filter_parts_c.append(industry)
    if region:
        filter_parts_c.append(region)
    filter_label_c = (
        "  ·  " + "  ·  ".join(filter_parts_c) if filter_parts_c else "  ·  Global"
    )

    lines = []

    # Section 1 — Header
    lines += [
        f"# {cloud} · Adoption Heatmap · {fy}",
        (
            f"_{total_accounts:,} accounts · {total_products} products · "
            f"{_now_ist()}{filter_label_c}_"
        ),
        "",
    ]

    # Section 2 — Summary bar
    lines += [
        (
            f"{green_n} :large_green_circle: Healthy · "
            f"{amber_n} :large_yellow_circle: Watch · "
            f"{red_n} :red_circle: Critical"
        ),
        "",
    ]

    # Section 3 — Heatmap table
    headers = ["Quarter"] + [
        _safe_cell(p, _PRODUCT_HEADER_LEN) for p in display_products
    ]
    lines.append("| " + " | ".join(headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for q in all_quarters:
        cells = [q]
        for p in display_products:
            row = lookup.get((p, q))
            if row:
                emoji = score_to_emoji(row.get("status", ""))
                score = row.get("score", 0)
                cells.append(f"{emoji} {score}%")
            else:
                cells.append(":white_circle: —")
        lines.append("| " + " | ".join(cells) + " |")

    if truncated:
        lines.append(
            f"_Showing top {max_products} products by account count_"
        )

    lines.append("")

    # Section 4 — Insights
    lines += [
        f"__Strongest:__ {top_product} ({top_score}%)",
        f"__Needs attention:__ {bottom_product} ({bottom_score}%)",
        "",
    ]

    # Section 5 — Drill-down prompt
    lines.append(
        "_Reply with a product name to see the account breakdown_"
    )

    canvas = "\n".join(lines)
    if len(canvas) > _MAX_HEATMAP_CHARS:
        canvas = canvas[:_MAX_HEATMAP_CHARS - 60]
        canvas += (
            "\n\n_Canvas truncated — reply with a product name to drill down._"
        )

    return canvas


def build_heatmap_canvas_markdown(
    features: list[dict],
    cloud: str,
    fy: str,
    thresholds: dict | None = None,
    snapshot_date: str = "",
    total_accounts: int = 0,
) -> str:
    """
    Builds canvas markdown for App Home heatmap canvas.
    Shows Feature Group | Usage % | Trend | Threshold
    No quarterly breakdown — single latest snapshot.
    """
    from collections import defaultdict

    t = thresholds or {"green": 20.0, "yellow": 5.0}

    # Aggregate by feature group
    groups = defaultdict(list)
    for f in features:
        group = f.get("feature_group") or f.get("group") or "Unknown"
        groups[group].append(f)

    rows = ""
    for group_name, feats in sorted(
        groups.items(),
        key=lambda x: sum(
            float(f.get("score") or f.get("adoption_pct") or 0) for f in x[1]
        )
        / len(x[1]),
        reverse=True,
    ):
        avg_pct = sum(
            float(f.get("score") or f.get("adoption_pct") or 0) for f in feats
        ) / max(len(feats), 1)

        avg_trend = sum(
            float(f.get("trend") or 0) for f in feats
        ) / max(len(feats), 1)

        # Threshold badge
        if avg_pct > t["green"]:
            threshold = ":large_green_circle: Above"
        elif avg_pct >= t["yellow"]:
            threshold = ":large_yellow_circle: Watch"
        else:
            threshold = ":red_circle: Below"

        # Trend arrow
        if avg_trend > 2:
            trend = f"↑ +{avg_trend:.0f}%"
        elif avg_trend < -2:
            trend = f"↓ {avg_trend:.0f}%"
        else:
            trend = "→ Stable"

        gcell = _safe_cell(str(group_name), 80)
        rows += f"| {gcell} | {avg_pct:.0f}% | {trend} | {threshold} |\n"

    body = f"""# {cloud} Adoption Heatmap · {fy}

_{snapshot_date} · {total_accounts:,} accounts · {len(features)} features_

| Feature Group | Usage % | Trend | Threshold |
|---|---|---|---|
{rows}
---
_Thresholds: :large_green_circle: >{t['green']}% Healthy · :large_yellow_circle: {t['yellow']}–{t['green']}% Watch · :red_circle: <{t['yellow']}% Critical_
_Source: PDP 2.0 · RPT\\_PRODUCTUSAGE\\_PFT\\_ORG\\_METRICS_
"""
    if len(body) > _MAX_HEATMAP_CHARS:
        return body[: _MAX_HEATMAP_CHARS - 60] + "\n\n_(Canvas truncated.)_"
    return body


def build_product_drilldown_canvas(
    product_data: list,
    product: str,
    cloud: str,
    fy: str,
) -> str:
    """
    Takes data for one product across all quarters and returns
    a drill-down Canvas markdown string for thread reply.
    """

    if not product_data:
        return (
            f"# {product} · {cloud} · {fy}\n\n"
            "_No data available for this product._"
        )

    sorted_rows = sorted(
        product_data,
        key=lambda r: r.get("quarter", "")[-2:]
    )

    latest = sorted_rows[-1]
    account_count = latest.get("account_count", 0)
    status = latest.get("status", "")
    status_badge = {
        "green": ":large_green_circle: Healthy",
        "amber": ":large_yellow_circle: Watch",
        "red": ":red_circle: Critical",
    }.get(status, ":white_circle: No data")

    lines = []

    # Section 1 — Header
    lines += [
        f"# {product} · {cloud} · {fy}",
        f"_{account_count:,} accounts · {status_badge}_",
        "",
    ]

    # Section 2 — Quarter table
    lines += [
        "| Quarter | Score | Utilization | Penetration | Trend |",
        "| --- | --- | --- | --- | --- |",
    ]

    for row in sorted_rows:
        q = row.get("quarter", "—")
        score = row.get("score", 0)
        utilization = row.get("utilization", 0.0)
        penetration = row.get("penetration", 0.0)
        trend = row.get("trend")
        emoji = score_to_emoji(row.get("status", ""))
        lines.append(
            f"| {q} | {emoji} {score}% "
            f"| {utilization * 100:.1f}% "
            f"| {penetration * 100:.1f}% "
            f"| {_trend_arrow(trend)} |"
        )

    lines.append("")

    # Section 3 — Narrative
    lines += [_narrative(sorted_rows), ""]

    # Section 4 — Owner prompt
    lines.append(
        "_To see accounts on this product, "
        "contact your Adoption PM or run `/gm-review-canvas`_"
    )

    canvas = "\n".join(lines)
    if len(canvas) > _MAX_DRILLDOWN_CHARS:
        canvas = canvas[:_MAX_DRILLDOWN_CHARS - 60]
        canvas += (
            "\n\n_Canvas truncated — "
            "contact your Adoption PM for full details._"
        )

    return canvas


def build_movers_section(movers_data: dict) -> str:
    """
    Builds the top movers + top losers section for a drill-down canvas.

    Args:
        movers_data: output of get_feature_account_movers()

    Returns:
        Markdown string to append to drill-down canvas.
    """
    top_movers = movers_data.get("top_movers", [])
    top_losers = movers_data.get("top_losers", [])

    if not top_movers and not top_losers:
        return "\n_No account movement data available for this feature._"

    lines = ["", "---", ""]

    # Top movers
    if top_movers:
        lines.append("## :chart_with_upwards_trend: Top Movers")
        for a in top_movers:
            lines.append(
                f":large_green_circle: *{a['acct_nm']}* "
                f"— +{a['mau_change_pct']:.1f}% · "
                f"{a['mau_current']:,} MAU"
                + (f" · {a['csm_name']}" if a['csm_name'] != 'Unassigned' else "")
            )
        lines.append("")

    # Top losers
    if top_losers:
        lines.append("## :chart_with_downwards_trend: Losing Ground")
        for a in top_losers:
            lines.append(
                f":red_circle: *{a['acct_nm']}* "
                f"— {a['mau_change_pct']:.1f}% · "
                f"{a['mau_current']:,} MAU"
                + (f" · {a['csm_name']}" if a['csm_name'] != 'Unassigned' else "")
            )
        lines.append("")

    return "\n".join(lines)


def build_home_loading_blocks(cloud: str) -> list:
    """Home tab loading state while heatmap data is fetched."""
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"⏳ Loading {cloud} heatmap..."},
        }
    ]


def build_home_cloud_header(cloud: str) -> list:
    """Home tab header for selected cloud heatmap."""
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"📊 {cloud} Adoption Heatmap"},
        },
        {"type": "divider"},
    ]


def build_home_refresh_button(cloud: str) -> list:
    """Home tab refresh action for the currently selected cloud."""
    return [
        {"type": "divider"},
        {
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": "🔄 Refresh"},
                "action_id": "home_cloud_select",
                "value": cloud,
            }],
        },
    ]


def build_adoption_heatmap_blocks(
    scored_data: list,
    cloud: str,
    fy: str,
    industry: str | None = None,
    region: str | None = None,
    title: str | None = None,
) -> list:
    """
    Builds Slack Block Kit blocks for adoption heatmap.
    Returns list of blocks ready for client.chat_postMessage(blocks=...)
    """
    from collections import defaultdict

    _, cloud_family = resolve_cloud(cloud)
    thresholds = get_thresholds(cloud_family)

    # Group features by feature_group
    groups = defaultdict(list)
    for f in scored_data:
        groups[f["feature_group"]].append(f)

    # Summary counts (status-based to match cloud threshold config)
    green_n = sum(1 for f in scored_data if f.get("status") == "green")
    amber_n = sum(1 for f in scored_data if f.get("status") == "amber")
    red_n = sum(1 for f in scored_data if f.get("status") == "red")
    total = len(scored_data)
    accounts = max((f.get("account_count", 0) for f in scored_data), default=0)

    # Pulse bar — 10 blocks proportional to red/amber/green
    def _pulse_bar(green, amber, red, total, length=10):
        if total == 0:
            return ":white_circle:" * length
        g = round(green / total * length)
        a = round(amber / total * length)
        r = length - g - a
        return (
            ":red_circle:" * max(0, r) +
            ":large_yellow_circle:" * max(0, a) +
            ":large_green_circle:" * max(0, g)
        )

    pulse = _pulse_bar(green_n, amber_n, red_n, total)

    # Group health status — worst feature in group drives color
    def _group_status(features):
        if any(f["status"] == "red" for f in features):
            return "red"
        if any(f["status"] == "amber" for f in features):
            return "amber"
        return "green"

    def _group_avg_score(features):
        scores = [f["score"] for f in features]
        return round(sum(scores) / len(scores)) if scores else 0

    # Sort groups: red first, then amber, then green
    status_order = {"red": 0, "amber": 1, "green": 2}
    sorted_groups = sorted(
        groups.items(),
        key=lambda x: (status_order[_group_status(x[1])], -_group_avg_score(x[1]))
    )

    GROUP_EMOJI = {
        "Markets/I18n": ":earth_africa:",
        "Buyer Groups": ":busts_in_silhouette:",
        "Pricing": ":label:",
        "Search": ":mag:",
        "Setup & User Tools": ":gear:",
        "Cart": ":shopping_trolley:",
        "Shipping": ":package:",
        "Product & Catalog": ":clipboard:",
        "B2B Payments": ":moneybag:",
        "Shopper Experience & Profiles": ":shopping_bags:",
        "Promotions": ":dart:",
        "Checkout": ":credit_card:",
        "Import/Export Tools": ":inbox_tray:",
        "Payments": ":credit_card:",
        "Agentforce for Shopping": ":robot_face:",
        "Analytics": ":bar_chart:",
        "Buyer Messaging": ":speech_balloon:",
        "Data Cloud for Commerce": ":cloud:",
        "Subscriptions": ":arrows_counterclockwise:",
        "Tax": ":receipt:",
    }

    # Strongest / needs attention
    all_sorted = sorted(scored_data, key=lambda f: f["score"], reverse=True)
    strongest = all_sorted[0] if all_sorted else None
    weakest = all_sorted[-1] if all_sorted else None

    # Timestamp
    now = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
    ts = now.strftime("%b %d %Y %H:%M IST")

    # Build filter label (industry/region) for header context
    filter_parts: list[str] = []
    if industry:
        filter_parts.append(industry)
    if region:
        filter_parts.append(region)
    filter_label = (
        "  ·  " + "  ·  ".join(filter_parts) if filter_parts else "  ·  Global"
    )

    # --- Build blocks ---
    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": title or f":chart_with_upwards_trend: {cloud} · Adoption Heatmap · {fy}",
            "emoji": True
        }
    })

    # Summary context
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"{accounts:,} accounts  ·  "
                f"{total} features  ·  "
                f"{ts}"
                f"{filter_label}"
            )
        }]
    })

    # Pulse bar + counts
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"{pulse}\n"
                f"*{green_n}* :large_green_circle: Healthy  ·  "
                f"*{amber_n}* :large_yellow_circle: Watch  ·  "
                f"*{red_n}* :red_circle: Critical"
            )
        }
    })
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f":large_green_circle: >={thresholds['green']:.1f}% Healthy  ·  "
                f":large_yellow_circle: >={thresholds['yellow']:.1f}% Watch  ·  "
                f":red_circle: <{thresholds['yellow']:.1f}% Critical"
            ),
        }],
    })

    blocks.append({"type": "divider"})

    # Group rows with drill-down buttons
    for group_name, features in sorted_groups:
        avg_score = _group_avg_score(features)
        count = len(features)
        total_group_accts = max(
            (f.get("account_count", 0) for f in features),
            default=0
        )
        group_emoji = GROUP_EMOJI.get(group_name, ":bookmark_tabs:")
        filled = round(avg_score / 10)
        score_bar = "█" * filled + "░" * (10 - filled)
        trend_badge = ""

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{group_emoji} *{group_name}*  {trend_badge}\n"
                    f"{total_group_accts:,} accounts  ·  "
                    f"{count} feature{'s' if count != 1 else ''}  ·  "
                    f"{score_bar}  {avg_score:.0f}% avg"
                )
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Drill down ↗",
                    "emoji": True
                },
                "action_id": "heatmap_drilldown",
                "value": f"{group_name}|{cloud}|{fy}"
            }
        })

    blocks.append({"type": "divider"})

    # Strongest / needs attention
    summary_text = ""
    if strongest:
        summary_text += (
            f":trophy: *Strongest:* {_feature_link(strongest)} "
            f"({strongest['score']}%)"
        )
    if weakest:
        summary_text += (
            f"\n:warning: *Needs attention:* {_feature_link(weakest)} "
            f"({weakest['score']}%)"
        )
    if summary_text:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary_text}
        })

    # Footer
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                "_Reply with any feature or group name for drill-down · "
                "Click *Drill down ↗* for group detail_"
            )
        }]
    })

    return blocks


STATUS_COLORS = {
    "green": "#2EB67D",
    "amber": "#ECB22E",
    "red": "#E01E5A",
}

STATUS_BADGES = {
    "green": ":white_check_mark: HEALTHY",
    "amber": ":warning: WATCH",
    "red": ":rotating_light: CRITICAL",
}

STATUS_EMOJI = {
    "green": ":large_green_circle:",
    "amber": ":large_yellow_circle:",
    "red": ":red_circle:",
}


def _score_bar(score: int) -> str:
    filled = round(score / 10)
    return "█" * filled + "░" * (10 - filled)


def _trend_arrow(trend) -> str:
    if trend is None:
        return "→ no prior data"
    if trend > 0:
        return f":arrow_upper_right: +{trend:.1f}%"
    return f":arrow_lower_right: {trend:.1f}%"


def _format_trend(trend) -> str:
    """Formats trend safely to avoid tiny-denominator explosions."""
    if trend is None:
        return "→ No prior data"
    if abs(trend) > 1000:
        return "🆕 New activity"
    if trend > 2:
        return f"↑ +{trend:.0f}%"
    if trend < -2:
        return f"↓ {trend:.0f}%"
    return "→ Stable"


def _status_from_score(score: int) -> str:
    if score >= 70:
        return "green"
    if score >= 40:
        return "amber"
    return "red"


def build_group_drilldown_blocks(
    group_features: list,
    group_name: str,
    cloud: str,
    fy: str,
    movers_data: dict = None,
    user_id: str | None = None,
    is_on_watchlist_fn=None,
) -> list:
    """
    Layer 2 — Group drill-down blocks.
    One section per feature with score bar + feature detail button.
    """
    del movers_data
    sorted_features = sorted(group_features, key=lambda f: f.get("score", 0))
    green_n = sum(1 for f in group_features if f.get("status") == "green")
    amber_n = sum(1 for f in group_features if f.get("status") == "amber")
    red_n = sum(1 for f in group_features if f.get("status") == "red")
    avg_score = round(
        sum(f.get("score", 0) for f in group_features) / len(group_features)
    ) if group_features else 0

    blocks = []
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f"{group_name} · {cloud} · {fy}", "emoji": True},
    })
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"*{avg_score}% avg*  ·  "
                f"*{len(group_features)} feature{'s' if len(group_features) != 1 else ''}*"
                f"\n{green_n} :large_green_circle:  "
                f"{amber_n} :large_yellow_circle:  "
                f"{red_n} :red_circle:"
            ),
        },
    })
    blocks.append({"type": "divider"})

    for f in sorted_features:
        feature_id = f.get("feature_id", "")
        blocks.extend(
            build_feature_card(
                f,
                feature_id,
                cloud,
                fy,
                user_id=user_id,
                is_on_watchlist_fn=is_on_watchlist_fn,
            )
        )

    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": "_Reply with a feature name for the full feature drill-down_",
        }],
    })
    return blocks


def _watchlist_button(
    user_id: str | None,
    feature_id: str,
    feature_name: str,
    cloud: str,
    is_on_watchlist_fn=None,
) -> dict:
    """Returns Watch or Remove button based on watchlist state."""
    on_watchlist = False
    if user_id and is_on_watchlist_fn:
        try:
            on_watchlist = bool(is_on_watchlist_fn(user_id, feature_id))
        except Exception:
            on_watchlist = False

    if on_watchlist:
        return {
            "type": "button",
            "text": {"type": "plain_text", "text": "❌ Remove from Watchlist"},
            "action_id": "remove_from_watchlist",
            "value": f"{feature_id}|{feature_name}|{cloud}",
            "style": "danger",
        }
    return {
        "type": "button",
        "text": {"type": "plain_text", "text": "👁 Watch"},
        "action_id": "add_to_watchlist",
        "value": f"{feature_id}|{feature_name}|{cloud}",
    }


def build_feature_card(
    feature,
    feature_id,
    cloud,
    fy,
    user_id: str | None = None,
    is_on_watchlist_fn=None,
):
    score = float(feature.get("score") or 0)
    mau = feature.get("mau") or 0
    trend_raw = feature.get("trend")
    trend = float(trend_raw) if trend_raw is not None else None
    name = feature.get("feature", "Unknown")

    # Health emoji
    health = (
        ":large_green_circle:" if score > 20
        else ":large_yellow_circle:" if score >= 5
        else ":red_circle:"
    )

    # MAU display
    mau_display = f"{mau/1000:.1f}K MAU" if mau >= 1000 else f"{mau} MAU"

    # Growth text
    growth = _format_trend(trend)

    # Insight line
    if score < 5:
        insight = ":warning: Critical — needs immediate attention"
    elif score < 10:
        insight = ":warning: Low adoption — review blockers"
    elif trend is not None and trend < -10:
        insight = ":warning: Significant drop — investigate"
    elif trend is not None and trend > 10:
        insight = ":rocket: Strong growth momentum"
    else:
        insight = ":bulb: Stable — monitor for opportunities"

    return [
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{health} {_feature_link(feature)}*\n"
                    f"{score:.0f}% adoption  -  {mau_display}  -  {growth}"
                )
            }
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": insight}]
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Feature Detail"},
                    "style": "primary",
                    "action_id": "heatmap_feature_detail",
                    "value": f"{feature_id}|{name}|{cloud}|{fy}"
                },
                _watchlist_button(user_id, feature_id, name, cloud, is_on_watchlist_fn),
            ]
        }
    ]


def build_feature_detail_blocks(
    feature: dict,
    movers: dict,
    cloud: str,
    fy: str,
    call_llm_fn=None,
    user_id: str | None = None,
    is_on_watchlist_fn=None,
) -> tuple[list, str]:
    score = float(feature.get("score") or 0)
    mau = feature.get("mau") or 0
    trend_raw = feature.get("trend")
    trend = float(trend_raw) if trend_raw is not None else None
    name = feature.get("feature", "Unknown")
    group = feature.get("feature_group", "")
    owner = feature.get("owner", "")
    description = feature.get("description", "")
    penetration = float(feature.get("penetration") or 0)
    feature_id = feature.get("feature_id", "")
    gus_url = feature.get("gus_url", "")
    availability = feature.get("availability", "GA")
    snapshot_date = feature.get("data_dt", "")
    accounts = int(feature.get("account_count") or 0)
    transactions = int(feature.get("transactions") or 0)

    # Health emoji + status
    if score > 20:
        health = ":large_green_circle:"
        status = "Performing well"
    elif score >= 5:
        health = ":large_yellow_circle:"
        status = "Needs attention"
    else:
        health = ":red_circle:"
        status = ":rotating_light: Critically underperforming"

    # MAU display
    mau_display = f"{mau/1000:.1f}K MAU" if mau >= 1000 else f"{mau} MAU"

    # Growth text
    growth = _format_trend(trend)

    # Why analysis — data-driven
    penetration_pct = penetration * 100
    why_lines = []
    if penetration_pct < 10:
        why_lines.append(
            f"- Low account penetration ({penetration_pct:.1f}%) — feature likely undiscovered"
        )
    if mau < 100:
        why_lines.append("- Very low active usage — may not be enabled by default")
    if trend is not None and trend < -10:
        why_lines.append("- Sharp decline — investigate recent product or UX changes")
    if score < 5:
        why_lines.append("- Below critical threshold — immediate action required")
    if not why_lines:
        why_lines.append("- Usage is stable but growth opportunity exists")
    why_text = "\n".join(why_lines)

    # What to do — actionable (LLM first, deterministic fallback)
    default_actions_text = (
        "- Review feature discoverability in UI\n"
        "- Identify top-adopting accounts and replicate patterns\n"
        "- Schedule PM review with CSM team"
    )
    if call_llm_fn:
        try:
            actions_text = call_llm_fn(
                prompt=(
                    f"You are a Salesforce Product Manager advisor.\n"
                    f"Feature: {name}\n"
                    f"Cloud: {cloud}\n"
                    f"Group: {group}\n"
                    f"Adoption: {score:.0f}%\n"
                    f"MAU: {mau}\n"
                    f"Penetration: {penetration:.1f}%\n"
                    f"Trend: {(_format_trend(trend))}\n"
                    f"Description: {description}\n\n"
                    f"Give exactly 3 specific, actionable recommendations for a PM "
                    f"to improve adoption of this feature. "
                    f"Each on a new line starting with '- '. "
                    f"Be specific to this feature, not generic. "
                    f"Max 15 words per recommendation."
                ),
                system_prompt=(
                    "You are a concise Salesforce PM advisor. "
                    "Give specific, actionable recommendations only. "
                    "No preamble, no explanation, just the 3 bullet points."
                ),
                max_tokens=150,
            )
            if not str(actions_text).strip():
                actions_text = default_actions_text
        except Exception:
            actions_text = default_actions_text
    else:
        actions_text = default_actions_text

    # Feature name with GUS link
    name_display = f"<{gus_url}|{name}>" if gus_url else name

    blocks = [
        # Header
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{health} {name}"}
        },
        # Context breadcrumb
        {
            "type": "context",
            "elements": [{"type": "mrkdwn",
                "text": f"{group}  ·  {cloud}  ·  {availability}  ·  {fy}  ·  as of {snapshot_date}"}]
        },
        # KPI + status
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{score:.0f}% adoption  ·  {mau_display}  ·  {growth}*"
                )
            }
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Accounts*\n:busts_in_silhouette: {accounts:,}"},
                {"type": "mrkdwn", "text": f"*Penetration*\n:dart: {penetration*100:.1f}%"},
                {"type": "mrkdwn", "text": f"*Transactions*\n:arrows_counterclockwise: {transactions:,}"},
                {"type": "mrkdwn", "text": f"*MAU (28d)*\n:bar_chart: {mau:,}"},
            ]
        },
        {"type": "divider"},
        # Why
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Why this is happening:*\n{why_text}"}
        },
        # What to do
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*What to do:*  _✨ AI recommendations_\n{actions_text}"}
        },
        {"type": "divider"},
    ]

    # About — only if description exists
    if description:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*About:*\n{description}"}
        })

    # Feature Owner
    if owner:
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Feature Owner*\n:bust_in_silhouette: {owner}"
            },
        })

    blocks.append({"type": "divider"})

    # Top movers — only if data exists
    top_movers = movers.get("top_movers", [])
    top_losers = movers.get("top_losers", [])

    if top_movers:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Top adopters:*"}
        })
        for m in top_movers[:3]:
            acct_name = m.get("account_name") or m.get("acct_nm") or "Unknown"
            acct_id = m.get("account_id") or m.get("acct_id") or ""
            delta = (
                m.get("delta_pct")
                if m.get("delta_pct") is not None
                else m.get("mau_change_pct", 0)
            )
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"↑ *{acct_name}*  `+{float(delta):.0f}%`"
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Heatmap →"},
                    "action_id": "account_feature_heatmap",
                    "value": f"{acct_id}|{acct_name}|{cloud}|{fy}|{feature_id}"
                }
            })

    if top_losers:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Losing ground:*"}
        })
        for l in top_losers[:3]:
            acct_name = l.get("account_name") or l.get("acct_nm") or "Unknown"
            acct_id = l.get("account_id") or l.get("acct_id") or ""
            delta = (
                l.get("delta_pct")
                if l.get("delta_pct") is not None
                else l.get("mau_change_pct", 0)
            )
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"↓ *{acct_name}*  `{float(delta):.0f}%`"
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Heatmap →"},
                    "action_id": "account_feature_heatmap",
                    "value": f"{acct_id}|{acct_name}|{cloud}|{fy}|{feature_id}"
                }
            })

    # Actions
    action_elements = [
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "✉️ Message Owner"},
            "style": "primary",
            "action_id": "heatmap_message_owner",
            "value": f"{feature_id}|{name}|{owner}|{cloud}"
        },
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "⚖️ Compare"},
            "action_id": "heatmap_compare",
            "value": f"{feature_id}|{name}|{cloud}|{fy}"
        },
        _watchlist_button(user_id, feature_id, name, cloud, is_on_watchlist_fn),
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "↩ Back to Group"},
            "action_id": "heatmap_back_to_group",
            "value": f"{group}|{cloud}|{fy}"
        }
    ]

    blocks.append({
        "type": "actions",
        "elements": action_elements
    })

    color = (
        "#1A7A45" if score > 20
        else "#8A6000" if score >= 5
        else "#C0392B"
    )

    del name_display
    return blocks, color


def _infer_root_causes(
    score: int,
    trend: float | None,
    acct_count: int,
    penetration: float,
) -> list[str]:
    """
    Infers likely root causes based on score + trend signals.
    Returns 2-3 bullet points for the Why Analysis section.
    """
    causes = []

    if penetration < 0.10:
        causes.append(
            "Low account penetration — feature may not be visible "
            "or enabled by default in new org setup"
        )
    elif penetration < 0.30:
        causes.append(
            "Below-average penetration — CSM-led enablement sessions "
            "likely needed to drive awareness"
        )

    if trend is not None and trend < -10:
        causes.append(
            f"Declining usage ({trend:.1f}% trend) — "
            "check recent release notes for breaking changes or UX regressions"
        )
    elif trend is not None and trend < 0:
        causes.append(
            "Slight usage decline — monitor for 2 more weeks "
            "before escalating"
        )

    if score < 30:
        causes.append(
            "Critical threshold breach — "
            "immediate CSM outreach recommended for at-risk accounts"
        )
    elif score < 50:
        causes.append(
            "Below watch threshold — "
            "targeted enablement content may improve adoption"
        )

    if acct_count < 10:
        causes.append(
            "Very low account count — "
            "feature may be in early rollout or require specific entitlement"
        )

    # Always have at least 2 causes
    if len(causes) < 2:
        causes.append(
            "No strong negative signal — "
            "continue monitoring and share success stories across accounts"
        )

    return causes[:3]


def _recommended_action(
    score: int,
    trend: float | None,
    acct_count: int,
    top_losers: list,
) -> str:
    """
    Generates a single specific recommended action based on signals.
    """
    del acct_count
    if score < 30 and top_losers:
        loser_nm = top_losers[0].get("acct_nm", "top at-risk account")
        return (
            f"Immediate intervention needed. "
            f"Start with *{loser_nm}* — "
            f"schedule a CSM-led setup session and review "
            f"feature configuration. "
            f"Use the mover accounts above as success story references."
        )
    elif score < 30:
        return (
            "Feature is critically underperforming. "
            "Review onboarding documentation and check if feature "
            "is enabled by default. "
            "Consider a targeted enablement campaign for all accounts."
        )
    elif trend is not None and trend < -15:
        return (
            "Usage is declining significantly. "
            "Review the last 2 product releases for breaking changes. "
            "Reach out to top loser accounts proactively before "
            "this appears in renewal risk."
        )
    elif score < 50:
        return (
            "Feature is in watch territory. "
            "Identify the top 5 accounts not yet using this feature "
            "and share success stories from your top mover accounts."
        )
    else:
        return (
            "Adoption is healthy — focus on expanding to accounts "
            "not yet activated. "
            "Share top mover case studies in the next CSM team meeting."
        )
