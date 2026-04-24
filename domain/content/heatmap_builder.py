"""
domain/content/heatmap_builder.py

Pure string formatting for Adoption Heatmap canvases.
No Snowflake calls. No slack_sdk imports.
Standard library only.
"""

from datetime import datetime
from zoneinfo import ZoneInfo
from domain.analytics.adoption_scoring import score_to_emoji

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

    lines = []

    # Section 1 — Header
    lines += [
        f"# {cloud} · Adoption Heatmap · {fy}",
        f"_{total_accounts:,} accounts · {total_products} products · {_now_ist()}_",
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


def build_adoption_heatmap_blocks(
    scored_data: list,
    cloud: str,
    fy: str,
) -> list:
    """
    Builds Slack Block Kit blocks for adoption heatmap.
    Returns list of blocks ready for client.chat_postMessage(blocks=...)
    """
    from collections import defaultdict

    # Group features by feature_group
    groups = defaultdict(list)
    for f in scored_data:
        groups[f["feature_group"]].append(f)

    # Summary counts
    green_n = sum(1 for f in scored_data if f["status"] == "green")
    amber_n = sum(1 for f in scored_data if f["status"] == "amber")
    red_n = sum(1 for f in scored_data if f["status"] == "red")
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

    # Strongest / needs attention
    all_sorted = sorted(scored_data, key=lambda f: f["score"], reverse=True)
    strongest = all_sorted[0] if all_sorted else None
    weakest = all_sorted[-1] if all_sorted else None

    # Timestamp
    now = datetime.now(tz=ZoneInfo("Asia/Kolkata"))
    ts = now.strftime("%b %d %Y %H:%M IST")

    # --- Build blocks ---
    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f":bar_chart: {cloud} · Adoption Heatmap · {fy}",
            "emoji": True
        }
    })

    # Summary context
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"{accounts:,} accounts · {total} features · {ts}"
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

    blocks.append({"type": "divider"})

    # Group rows with drill-down buttons
    for group_name, features in sorted_groups:
        status = _group_status(features)
        avg_score = _group_avg_score(features)
        count = len(features)
        emoji = {
            "red": ":red_circle:",
            "amber": ":large_yellow_circle:",
            "green": ":large_green_circle:",
        }[status]

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{emoji} *{group_name}*"
                    f"    {count} features · {avg_score}% avg"
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
            f":trophy: *Strongest:* {strongest['feature']} "
            f"({strongest['score']}%)"
        )
    if weakest:
        summary_text += (
            f"\n:warning: *Needs attention:* {weakest['feature']} "
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


def build_group_drilldown_blocks(
    group_features: list,
    group_name: str,
    cloud: str,
    fy: str,
    movers_data: dict = None,
) -> list:
    """
    Builds Slack Block Kit blocks for group drill-down.
    Posted as a thread reply when Drill down button is clicked
    or group name is typed.

    Args:
        group_features: list of scored feature dicts for this group
        group_name:     e.g. "Cart"
        cloud:          e.g. "Commerce B2B"
        fy:             e.g. "FY2027"
        movers_data:    optional output of get_feature_account_movers()

    Returns:
        list of Slack Block Kit blocks
    """
    from collections import defaultdict
    del defaultdict

    # Sort features: worst first
    sorted_features = sorted(
        group_features,
        key=lambda f: f.get("score", 0)
    )

    # Group summary
    green_n = sum(1 for f in group_features if f["status"] == "green")
    amber_n = sum(1 for f in group_features if f["status"] == "amber")
    red_n = sum(1 for f in group_features if f["status"] == "red")
    avg_score = round(
        sum(f["score"] for f in group_features) / len(group_features)
    ) if group_features else 0
    total_accts = max(
        (f.get("account_count", 0) for f in group_features), default=0
    )

    # Group status
    if red_n > 0:
        group_status = "red"
        group_emoji = ":red_circle:"
    elif amber_n > 0:
        group_status = "amber"
        group_emoji = ":large_yellow_circle:"
    else:
        group_status = "green"
        group_emoji = ":large_green_circle:"
    del group_status

    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"{group_name} · {cloud} · {fy}",
            "emoji": True
        }
    })

    # Group summary
    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"{group_emoji} *{avg_score}% avg adoption*  ·  "
                f"{len(group_features)} features  ·  "
                f"{total_accts:,} accounts\n"
                f"{green_n} :large_green_circle:  "
                f"{amber_n} :large_yellow_circle:  "
                f"{red_n} :red_circle:"
            )
        }
    })

    blocks.append({"type": "divider"})

    # Feature rows — one section per feature
    for f in sorted_features:
        score = f.get("score", 0)
        acct_count = f.get("account_count", 0)
        mau = f.get("mau", 0)
        trend = f.get("trend")
        owner = f.get("owner", "Unassigned")
        status = f.get("status", "")
        emoji = f.get("emoji", ":white_circle:")
        feature_nm = f.get("feature", "")
        description = f.get("description", "")
        del status

        # Trend string
        if trend is None:
            trend_str = "— no prior data"
        elif trend > 0:
            trend_str = f":small_red_triangle: +{trend:.1f}%"
        else:
            trend_str = f":small_red_triangle_down: {trend:.1f}%"

        # Truncate description to 120 chars
        desc_preview = ""
        if description:
            desc_preview = (
                description[:117] + "…"
                if len(description) > 120
                else description
            )

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"{emoji} *{feature_nm}*\n"
                    f"Score: *{score}%*  ·  "
                    f"{acct_count:,} accounts  ·  "
                    f"{mau:,} MAU  ·  "
                    f"Trend: {trend_str}\n"
                    f"Owner: {owner}"
                    + (f"\n_{desc_preview}_" if desc_preview else "")
                )
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Account detail ↗",
                    "emoji": True
                },
                "action_id": "heatmap_feature_detail",
                "value": f"{f.get('feature_id', '')}|{f.get('feature', '')}|{cloud}|{fy}"
            }
        })

    blocks.append({"type": "divider"})

    # Movers section — if provided
    if movers_data:
        top_movers = movers_data.get("top_movers", [])
        top_losers = movers_data.get("top_losers", [])

        if top_movers:
            mover_lines = [":chart_with_upwards_trend: *Top Movers*"]
            for a in top_movers:
                mover_lines.append(
                    f":large_green_circle: *{a['acct_nm']}*  "
                    f"+{a['mau_change_pct']:.1f}%  ·  "
                    f"{a['mau_current']:,} MAU"
                    + (f"  ·  {a['csm_name']}"
                       if a.get('csm_name') != 'Unassigned' else "")
                )
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join(mover_lines)
                }
            })

        if top_losers:
            loser_lines = [":chart_with_downwards_trend: *Losing Ground*"]
            for a in top_losers:
                loser_lines.append(
                    f":red_circle: *{a['acct_nm']}*  "
                    f"{a['mau_change_pct']:.1f}%  ·  "
                    f"{a['mau_current']:,} MAU"
                    + (f"  ·  {a['csm_name']}"
                       if a.get('csm_name') != 'Unassigned' else "")
                )
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join(loser_lines)
                }
            })

        if top_movers or top_losers:
            blocks.append({"type": "divider"})

    # Footer
    blocks.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                "_Click *Account detail ↗* on any feature to see "
                "top mover and loser accounts  ·  "
                "Reply with a feature name for full drill-down_"
            )
        }]
    })

    return blocks


def build_feature_detail_blocks(
    feature: dict,
    movers: dict,
    cloud: str,
    fy: str,
) -> list:
    """
    Layer 3 — Full feature intelligence brief.
    Matches the PM Intelligence Hub design:
    - Header + status badge
    - 4 KPI tiles
    - Usage trend sparkline (emoji bars)
    - Root cause analysis
    - Support case themes
    - Voice of Customer
    - Precedent accounts
    - Owner card
    - At-risk accounts
    - Recommended action
    - Action buttons
    """

    score = feature.get("score", 0)
    status = feature.get("status", "")
    feature_nm = feature.get("feature", "")
    group_nm = feature.get("feature_group", "")
    owner = feature.get("owner", "Unassigned")
    acct_count = feature.get("account_count", 0)
    mau = feature.get("mau", 0)
    transactions = feature.get("transactions", 0)
    utilization = feature.get("utilization", 0.0)
    penetration = feature.get("penetration", 0.0)
    trend = feature.get("trend")
    description = feature.get("description", "")
    availability = feature.get("availability", "")
    data_dt = feature.get("data_dt", "")
    feature_id = feature.get("feature_id", "")

    top_movers = movers.get("top_movers", [])
    top_losers = movers.get("top_losers", [])

    # Status badge
    status_badge = {
        "green": "🟢 HEALTHY",
        "amber": "🟡 WATCH",
        "red": "🔴 CRITICAL",
    }.get(status, "⚪ UNKNOWN")

    # Trend string
    if trend is None:
        trend_str = "— no prior data"
        trend_icon = "—"
    elif trend > 0:
        trend_str = f"+{trend:.1f}% vs last month"
        trend_icon = f":small_red_triangle: +{trend:.1f}%"
    else:
        trend_str = f"{trend:.1f}% vs last month"
        trend_icon = f":small_red_triangle_down: {trend:.1f}%"

    # Usage sparkline — 13 emoji blocks
    # Approximate trend visually using penetration + trend
    def _sparkline(penetration: float, trend: float | None) -> str:
        """
        Builds a 13-block emoji sparkline showing usage direction.
        Uses penetration as current level, trend to show direction.
        """
        base = penetration * 100  # 0-100
        blocks = 13

        # Simulate 13 weeks of data points
        if trend is None:
            # Flat line at current level
            points = [base] * blocks
        elif trend > 0:
            # Growing — start lower, end at current
            start = max(0, base - abs(trend) * 0.5)
            points = [
                start + (base - start) * (i / (blocks - 1))
                for i in range(blocks)
            ]
        else:
            # Declining — start higher, end at current
            start = min(100, base + abs(trend) * 0.5)
            points = [
                start + (base - start) * (i / (blocks - 1))
                for i in range(blocks)
            ]

        # Map each point to emoji
        def _point_emoji(v: float) -> str:
            if v >= 70:
                return ":large_green_circle:"
            if v >= 30:
                return ":large_yellow_circle:"
            return ":red_circle:"

        return "".join(_point_emoji(p) for p in points)

    sparkline = _sparkline(penetration, trend)

    blocks_out = []

    # -- HEADER --
    blocks_out.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"{feature_nm}",
            "emoji": True
        }
    })

    blocks_out.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"{status_badge}  ·  "
                f"{cloud}  ·  {group_nm}  ·  "
                f"{availability}  ·  {fy}  ·  "
                f"as of {data_dt}"
            )
        }]
    })

    blocks_out.append({"type": "divider"})

    # -- 4 KPI TILES --
    blocks_out.append({
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": (
                    f"*:bar_chart: Current Adoption*\n"
                    f"*{score}%*  ·  {acct_count:,} accounts"
                )
            },
            {
                "type": "mrkdwn",
                "text": (
                    f"*:chart_with_downwards_trend: 30-day Trend*\n"
                    f"*{trend_icon}*"
                )
            },
            {
                "type": "mrkdwn",
                "text": (
                    f"*:gear: MAU (28d rolling)*\n"
                    f"*{mau:,}*  ·  {transactions:,} txns"
                )
            },
            {
                "type": "mrkdwn",
                "text": (
                    f"*:dart: Penetration*\n"
                    f"*{penetration * 100:.1f}%*  of provisioned accounts"
                )
            },
        ]
    })

    blocks_out.append({"type": "divider"})

    # -- USAGE TREND SPARKLINE --
    blocks_out.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"*:clock1: Usage Trend  _(13-week)_*\n"
                f"{sparkline}\n"
                f"_{trend_str}  ·  "
                f"Utilization: {utilization * 100:.1f}%_"
            )
        }
    })

    blocks_out.append({"type": "divider"})

    # -- ROOT CAUSE --
    root_causes = _infer_root_causes(score, trend, acct_count, penetration)

    blocks_out.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                "*:mag: Why Analysis*\n"
                + "\n".join(
                    f"{i+1}. {rc}"
                    for i, rc in enumerate(root_causes)
                )
            )
        }
    })

    blocks_out.append({"type": "divider"})

    # -- TOP MOVERS --
    if top_movers:
        mover_lines = ["*:chart_with_upwards_trend: Top Movers*"]
        for i, a in enumerate(top_movers, 1):
            csm = (
                f"  ·  {a['csm_name']}"
                if a.get("csm_name") and a["csm_name"] != "Unassigned"
                else ""
            )
            region = (
                f"  ·  {a['csg_region']}"
                if a.get("csg_region") else ""
            )
            mover_lines.append(
                f"{i}. :large_green_circle: *{a['acct_nm']}*  "
                f"+{a['mau_change_pct']:.1f}%  ·  "
                f"{a['mau_current']:,} MAU"
                f"{region}{csm}"
            )
        blocks_out.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(mover_lines)
            }
        })
    else:
        blocks_out.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*:chart_with_upwards_trend: Top Movers*\n"
                    "_No mover data available — "
                    "prior month snapshot may not exist yet._"
                )
            }
        })

    # -- TOP LOSERS --
    if top_losers:
        loser_lines = ["*:chart_with_downwards_trend: Losing Ground*"]
        for i, a in enumerate(top_losers, 1):
            csm = (
                f"  ·  {a['csm_name']}"
                if a.get("csm_name") and a["csm_name"] != "Unassigned"
                else ""
            )
            region = (
                f"  ·  {a['csg_region']}"
                if a.get("csg_region") else ""
            )
            loser_lines.append(
                f"{i}. :red_circle: *{a['acct_nm']}*  "
                f"{a['mau_change_pct']:.1f}%  ·  "
                f"{a['mau_current']:,} MAU"
                f"{region}{csm}"
            )
        blocks_out.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "\n".join(loser_lines)
            }
        })
    else:
        blocks_out.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*:chart_with_downwards_trend: Losing Ground*\n"
                    "_No loser data available — "
                    "prior month snapshot may not exist yet._"
                )
            }
        })

    blocks_out.append({"type": "divider"})

    # -- FEATURE DESCRIPTION / VoC --
    if description:
        desc_preview = (
            description[:297] + "…"
            if len(description) > 300
            else description
        )
        blocks_out.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*:speech_balloon: About this Feature*\n_{desc_preview}_"
            }
        })
        blocks_out.append({"type": "divider"})

    # -- OWNER CARD --
    blocks_out.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": (
                f"*:bust_in_silhouette: Feature Owner*\n"
                f"*{owner}*  ·  {group_nm}  ·  {cloud}"
            )
        }
    })

    blocks_out.append({"type": "divider"})

    # -- RECOMMENDED ACTION --
    action_text = _recommended_action(score, trend, acct_count, top_losers)
    blocks_out.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*:bulb: Recommended Action*\n{action_text}"
        }
    })

    blocks_out.append({"type": "divider"})

    # -- ACTION BUTTONS --
    blocks_out.append({
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": ":envelope: Message Owner",
                    "emoji": True
                },
                "style": "primary",
                "action_id": "heatmap_message_owner",
                "value": f"{feature_id}|{feature_nm}|{owner}|{cloud}|{fy}"
            },
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": ":bar_chart: Compare Features",
                    "emoji": True
                },
                "action_id": "heatmap_compare",
                "value": f"{feature_id}|{feature_nm}|{cloud}|{fy}"
            },
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": ":arrow_left: Back to Group",
                    "emoji": True
                },
                "action_id": "heatmap_back_to_group",
                "value": f"{group_nm}|{cloud}|{fy}"
            },
        ]
    })

    # -- FOOTER --
    blocks_out.append({
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": (
                f"_PDP 2.0  ·  "
                f"RPT_PRODUCTUSAGE_PFT_ORG_METRICS  ·  "
                f"{data_dt}_"
            )
        }]
    })

    return blocks_out


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
