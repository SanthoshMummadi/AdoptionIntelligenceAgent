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

CLOUD_FEATURE_GROUPS = {
    "Commerce B2B": [
        "Cart", "Checkout", "Pricing", "Search", "Shipping",
        "Product & Catalog", "Buyer Groups", "B2B Payments",
        "Payments", "Promotions", "Markets/I18n", "Setup & User Tools",
        "Shopper Experience & Profiles", "Import/Export Tools",
        "Subscriptions", "Tax", "Analytics", "Buyer Messaging",
        "Data Cloud for Commerce", "Agentforce for Shopping",
    ],
    "Agentforce IT Service": [
        "CMDB & Service Graph", "AI Agents", "Service Management",
        "Knowledge Management", "Asset Management",
    ],
}

CLOUD_TOTAL_FEATURES = {
    "Commerce B2B": 83,
    "Agentforce IT Service": 16,
}


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
    total_accounts: int = 0,
    snapshot_date: str | None = None,
) -> list:
    """
    Main adoption heatmap — all accounts view.
    Structure: Header → Focus Now → Strong Areas → All Groups (ranked)
    """
    from collections import defaultdict

    del industry, region
    features = scored_data

    if not features:
        return [{"type": "section", "text": {"type": "mrkdwn",
            "text": f":x: No adoption data found for *{cloud}* · {fy}"}}]

    t = {"green": 20.0, "yellow": 5.0}

    groups = defaultdict(list)
    for f in features:
        groups[f.get("feature_group", "Unknown")].append(f)

    group_data = {}
    for group, feats in groups.items():
        avg = sum(float(f.get("score") or 0) for f in feats) / len(feats)
        trend = sum(float(f.get("trend") or 0) for f in feats) / len(feats)
        group_data[group] = {
            "avg": avg,
            "trend": trend,
            "count": len(feats),
            "health": (
                "green" if avg > t["green"]
                else "yellow" if avg > t["yellow"]
                else "red"
            )
        }

    all_sorted = sorted(group_data.items(), key=lambda x: x[1]["avg"])
    all_sorted_desc = list(reversed(all_sorted))

    healthy_count = sum(1 for d in group_data.values() if d["health"] == "green")
    watch_count = sum(1 for d in group_data.values() if d["health"] == "yellow")
    critical_count = sum(1 for d in group_data.values() if d["health"] == "red")

    if not total_accounts:
        total_accounts = max((f.get("account_count") or 0 for f in features), default=0)
    if not snapshot_date:
        snapshot_date = features[0].get("data_dt", "") if features else ""
    overall_avg = sum(d["avg"] for d in group_data.values()) / len(group_data)

    focus_groups = sorted(
        group_data.items(),
        key=lambda x: (x[1]["avg"] - (20 if x[1]["trend"] < -10 else 0))
    )[:3]
    strong_groups = all_sorted_desc[:3]

    del overall_avg

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f":bar_chart: {cloud} · Adoption Overview"}
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn",
                "text": (
                    f"{total_accounts:,} accounts  ·  "
                    f"{len(features)} features  ·  {snapshot_date}\n"
                    f":large_green_circle: {healthy_count} Healthy  ·  "
                    f":large_yellow_circle: {watch_count} Watch  ·  "
                    f":red_circle: {critical_count} Critical"
                )}]
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn",
                "text": (
                    f":large_green_circle: >{t['green']:.0f}% Healthy  ·  "
                    f":large_yellow_circle: >{t['yellow']:.0f}% Watch  ·  "
                    f":red_circle: <{t['yellow']:.0f}% Critical"
                )}]
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*:rotating_light: Focus Now*\n" +
                    "\n".join(
                        f"- `{g}` — {d['avg']:.0f}%"
                        + (f"  ↓ {d['trend']:.0f}%" if d["trend"] < -2 else "")
                        for g, d in focus_groups
                    )
                )
            },
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "View All →"},
                "action_id": "heatmap_drilldown_red_0",
                "value": f"{focus_groups[0][0]}|{cloud}|{fy}"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*:trophy: Strong Areas*\n" +
                    "\n".join(
                        f"- `{g}` — {d['avg']:.0f}%"
                        for g, d in strong_groups
                    )
                )
            }
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*All Feature Groups*"}
        },
    ]

    for i, (group, data) in enumerate(all_sorted_desc):
        health_emoji = (
            ":large_green_circle:" if data["health"] == "green"
            else ":large_yellow_circle:" if data["health"] == "yellow"
            else ":red_circle:"
        )
        row = (
            f"{health_emoji} *{group}*   "
            f"{data['count']} features  ·  "
            f"`Adoption: {data['avg']:.0f}%`"
        )

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": row
            },
            "accessory": {
                "type": "button",
                "text": {"type": "plain_text", "text": "Drill →"},
                "action_id": f"heatmap_drilldown_grp_{i}",
                "value": f"{group}|{cloud}|{fy}"
            }
        })

    blocks += [
        {"type": "divider"},
        {
            "type": "actions",
            "elements": [{
                "type": "button",
                "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Refresh"},
                "action_id": "heatmap_refresh",
                "value": f"{cloud}|{fy}"
            }]
        }
    ]

    return blocks


def build_account_heatmap_blocks(
    features: list,
    cloud: str,
    fy: str,
    account_name: str,
    snapshot_date: str | None = None,
) -> list:
    """
    Account-scoped heatmap — variance-aware layout.
    Low variance (all healthy) → premium summary card
    High variance (mixed) → split by health with drill buttons
    """
    from collections import defaultdict

    if not features:
        return [{"type": "section", "text": {"type": "mrkdwn",
            "text": f":x: No adoption data found for *{account_name}* · {cloud} · {fy}"}}]

    t = {"green": 20.0, "yellow": 5.0}
    if not snapshot_date:
        snapshot_date = features[0].get("data_dt", "") if features else ""

    # Aggregate by group
    groups = defaultdict(list)
    for f in features:
        groups[f.get("feature_group", "Unknown")].append(f)

    group_data = {}
    for group, feats in groups.items():
        avg = sum(float(f.get("score") or 0) for f in feats) / max(len(feats), 1)
        group_data[group] = {
            "avg": avg,
            "count": len(feats),
            "health": (
                "green" if avg > t["green"]
                else "yellow" if avg > t["yellow"]
                else "red"
            )
        }

    scores = [d["avg"] for d in group_data.values()]
    variance = max(scores) - min(scores) if scores else 0
    overall_avg = sum(scores) / len(scores) if scores else 0
    all_healthy = all(d["health"] == "green" for d in group_data.values())

    healthy_count = sum(1 for d in group_data.values() if d["health"] == "green")
    watch_count = sum(1 for d in group_data.values() if d["health"] == "yellow")
    critical_count = sum(1 for d in group_data.values() if d["health"] == "red")

    # Header — always shown
    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text",
                "text": f":office: {account_name}"}
        },
        {
            "type": "context",
            "elements": [{"type": "mrkdwn",
                "text": (
                    f"{len(features)} features  ·  {cloud}  ·  {fy}  ·  {snapshot_date}\n"
                    f":large_green_circle: {healthy_count} Healthy  ·  "
                    f":large_yellow_circle: {watch_count} Watch  ·  "
                    f":red_circle: {critical_count} Critical"
                )}]
        },
        {"type": "divider"}
    ]

    # -- LOW VARIANCE — all healthy --
    if all_healthy and variance < 20:

        # Coverage gap analysis
        total_possible = CLOUD_TOTAL_FEATURES.get(cloud, len(features))
        activated = len(features)
        gap = total_possible - activated
        coverage_pct = (activated / total_possible * 100) if total_possible > 0 else 100

        # Missing groups
        activated_groups = set(group_data.keys())
        all_possible_groups = CLOUD_FEATURE_GROUPS.get(cloud, [])
        missing_groups = [g for g in all_possible_groups if g not in activated_groups]

        # Active groups — sorted by score desc
        top_groups = sorted(
            group_data.keys(),
            key=lambda g: group_data[g]["avg"],
            reverse=True
        )[:5]

        blocks += [
            # Adoption status
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*:large_green_circle: {overall_avg:.0f}% adoption "
                        f"on activated features*\n"
                        f"No risks detected on {activated} active features"
                    )
                }
            },
            # Coverage gap — the KEY insight
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*:bar_chart: Feature Coverage: "
                        f"{activated}/{total_possible} ({coverage_pct:.0f}%)*\n"
                        + (
                            f":warning: *{gap} features untapped* — "
                            f"biggest growth opportunity is feature expansion"
                            if gap > 0 else
                            ":tada: Full feature coverage!"
                        )
                    )
                }
            },
        ]

        # Active groups
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*:white_check_mark: Active Groups ({len(group_data)})*\n"
                    + "  ·  ".join(
                        f"`{g}`"
                        for g in top_groups
                    )
                    + (f"  ·  _+{len(group_data)-5} more_"
                       if len(group_data) > 5 else "")
                )
            }
        })

        # Missing groups — only if there are any
        if missing_groups:
            blocks.append({"type": "divider"})
            missing_display = missing_groups[:8]  # cap at 8
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*:x: Missing Groups ({len(missing_groups)})*\n"
                        + "  ·  ".join(f"`{g}`" for g in missing_display)
                        + (f"\n_...and {len(missing_groups)-8} more_"
                           if len(missing_groups) > 8 else "")
                    )
                }
            })

        blocks.append({"type": "divider"})

        # Insight line
        if gap > 20:
            insight = (
                f"Account uses only {coverage_pct:.0f}% of {cloud} features — "
                f"activate missing groups to unlock full platform value"
            )
        elif gap > 0:
            insight = (
                f"Strong adoption across {activated} features — "
                f"{gap} additional features available to expand"
            )
        else:
            insight = (
                f"Full feature activation achieved — "
                f"focus on usage depth and MAU growth"
            )

        blocks.append({
            "type": "context",
            "elements": [{"type": "mrkdwn",
                "text": f":bulb: _Insight: {insight}_"}]
        })

    # -- HIGH VARIANCE — mixed health --
    else:
        red_groups = {g: d for g, d in group_data.items() if d["health"] == "red"}
        yellow_groups = {g: d for g, d in group_data.items() if d["health"] == "yellow"}
        green_groups = {g: d for g, d in group_data.items() if d["health"] == "green"}

        # Critical
        if red_groups:
            red_text = "\n".join(
                f":red_circle: `{g}` - {d['avg']:.0f}%  ·  {d['count']} features"
                for g, d in sorted(red_groups.items(), key=lambda x: x[1]["avg"])
            )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn",
                    "text": f"*:rotating_light: Needs Immediate Attention*\n{red_text}"}
            })
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": f"Drill {g[:20]} ->"},
                        "action_id": f"heatmap_drilldown_red_{i}",
                        "value": f"{g}|{cloud}|{fy}",
                        "style": "danger"
                    }
                    for i, g in enumerate(list(red_groups.keys())[:3])
                ]
            })
            blocks.append({"type": "divider"})

        # Watch
        if yellow_groups:
            yellow_text = "\n".join(
                f":large_yellow_circle: `{g}` - {d['avg']:.0f}%  ·  {d['count']} features"
                for g, d in sorted(yellow_groups.items(), key=lambda x: x[1]["avg"])
            )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn",
                    "text": f"*:dart: Watch*\n{yellow_text}"}
            })
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": f"Drill {g[:20]} ->"},
                        "action_id": f"heatmap_drilldown_yellow_{i}",
                        "value": f"{g}|{cloud}|{fy}"
                    }
                    for i, g in enumerate(list(yellow_groups.keys())[:3])
                ]
            })
            blocks.append({"type": "divider"})

        # Healthy — collapsed
        if green_groups:
            green_text = "  ·  ".join(
                f"`{g}` {d['avg']:.0f}%"
                for g, d in sorted(
                    green_groups.items(),
                    key=lambda x: x[1]["avg"],
                    reverse=True
                )
            )
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn",
                    "text": f"*:large_green_circle: Fully Adopted*\n{green_text}"}
            })
            blocks.append({"type": "divider"})

        # Insight
        if red_groups or yellow_groups:
            problem_areas = list(red_groups.keys()) + list(yellow_groups.keys())
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn",
                    "text": (
                        f":bulb: _Insight: "
                        f"{len(red_groups)} critical, {len(yellow_groups)} watch - "
                        f"focus on `{problem_areas[0]}` first for highest impact_"
                    )}]
            })

    # Footer
    blocks.append({
        "type": "actions",
        "elements": [{
            "type": "button",
            "text": {"type": "plain_text", "text": ":arrows_counterclockwise: Refresh"},
            "action_id": "heatmap_refresh",
            "value": f"{cloud}|{fy}"
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


def _format_mover_pct(pct) -> str:
    """Display MAU change % without tiny-denominator blow-ups (mirrors _format_trend cap)."""
    if pct is None:
        return ""
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return ""
    if abs(p) > 1000:
        return "🆕 New"
    if p > 0:
        return f"+{p:.0f}%"
    return f"{p:.0f}%"


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

    # Insight line (align with _format_trend: huge % = new baseline, not "growth")
    if score < 5:
        insight = ":warning: Critical — needs immediate attention"
    elif score < 10:
        insight = ":warning: Low adoption — review blockers"
    elif trend is not None and abs(trend) > 1000:
        insight = "🆕 New feature activity — baseline forming"
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
            dp = _format_mover_pct(delta) or "—"
            mn = str(acct_name or "Unknown").replace("|", " ")
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"↑ *{acct_name}*  `{dp}`"
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Heatmap →"},
                    "action_id": "account_feature_heatmap",
                    "value": f"{acct_id}|{cloud}|{fy}|{feature_id}|{mn}",
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
            dp = _format_mover_pct(delta) or "—"
            ln = str(acct_name or "Unknown").replace("|", " ")
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"↓ *{acct_name}*  `{dp}`"
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Heatmap →"},
                    "action_id": "account_feature_heatmap",
                    "value": f"{acct_id}|{cloud}|{fy}|{feature_id}|{ln}",
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
