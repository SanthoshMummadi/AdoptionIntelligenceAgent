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
