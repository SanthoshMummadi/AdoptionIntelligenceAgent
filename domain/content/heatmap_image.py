"""
domain/content/heatmap_image.py
Generate PNG heatmap images for Slack Home.
"""

from collections import defaultdict
from datetime import datetime
from io import BytesIO
import logging
import unicodedata

import matplotlib

matplotlib.use("Agg")  # headless backend for server/runtime environments
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

from domain.analytics.threshold_config import get_thresholds

logger = logging.getLogger(__name__)

def _norm_group_label(f: dict) -> str:
    g = f.get("group")
    if g is None or g == "":
        g = f.get("feature_group")
    s = unicodedata.normalize("NFKC", str(g) if g is not None else "")
    s = " ".join(s.split())
    return s or "Unknown"


def merge_heatmap_quarter_feature_rows(quarters: dict) -> list[dict]:
    """One row per feature_id, keeping the row with the latest data_dt (newest)."""
    by_id: dict[str, dict] = {}
    for _q in ("Q1", "Q2", "Q3", "Q4"):
        for f in quarters.get(_q) or []:
            fid = f.get("feature_id")
            if not fid:
                alt = f.get("feature") or ""
                fid = f"__noid__{alt}" if alt else ""
            if not fid:
                continue
            cur = by_id.get(fid)
            if cur is None:
                by_id[fid] = f
                continue
            d_new = str(f.get("data_dt") or "")
            d_old = str(cur.get("data_dt") or "")
            if d_new > d_old:
                by_id[fid] = f
    return list(by_id.values())


def _heatmap_subtitle(cloud: str, fy: str, n: int) -> str:
    parts: list[str] = []
    c = (cloud or "").strip()
    fyy = (fy or "").strip()
    if c:
        parts.append(c)
    if fyy:
        parts.append(fyy)
    parts.append(f"{n} features")
    return " · ".join(parts)


HEATMAP_IMAGE_CACHE = {
    "file_id": None,
    "generated_at": None,
    "cloud": None,
    "fy": None,
}


def _normalize_scored_data(scored_data: list[dict]) -> list[dict]:
    """Normalizes feature dict keys from existing workflow to renderer format."""
    normalized = []
    for f in scored_data:
        score = float(f.get("adoption_pct", f.get("score", 0.0)) or 0.0)
        normalized.append(
            {
                "group": _norm_group_label(f),
                "adoption_pct": score,
                "score": score,
                "trend": f.get("trend"),
                "trend_pct": float(
                    f.get("trend_pct", f.get("trend", f.get("pct_change", 0.0))) or 0.0
                ),
            }
        )
    return normalized


def generate_heatmap_image(
    scored_data: list[dict],
    cloud: str,
    fy: str,
    thresholds: dict = None,
) -> bytes:
    """
    Generates a table-style adoption heatmap PNG.
    """
    if not scored_data:
        raise ValueError("No scored data provided for heatmap image generation")
    logger.warning(
        f"SAMPLE FEATURE KEYS: {list(scored_data[0].keys()) if scored_data else 'empty'}"
    )
    logger.warning(
        "SAMPLE TREND VALUE: "
        f"{scored_data[0].get('trend_pct') or scored_data[0].get('trend') or scored_data[0].get('pct_change')}"
    )

    t = thresholds or {"green": 20.0, "yellow": 5.0}
    rows_in = _normalize_scored_data(scored_data)

    # ── COLORS ─────────────────────────────────────────
    CARD_BG = "#FFFFFF"
    BORDER_COLOR = "#E8ECF0"
    HEADER_BG = "#F0F4FF"
    GREEN_BG = "#EAF3DE"
    GREEN_FG = "#1A1D21"
    YELLOW_BG = "#FAEEDA"
    YELLOW_FG = "#1A1D21"
    RED_BG = "#FCEBEB"
    RED_FG = "#1A1D21"
    GRAY_BG = "#555555"
    GRAY_FG = "#FFFFFF"
    TITLE_COLOR = "#1A1D21"
    LABEL_COLOR = "#1A1D21"
    HEADER_COLOR = "#444444"
    ACCENT_COLOR = "#0B5CFF"
    ACCENT_BG = "#E8F0FE"

    groups = defaultdict(list)
    for f in rows_in:
        groups[f["group"]].append(f)

    rows = []
    for group_name, features in groups.items():
        avg_adoption = sum(float(f.get("adoption_pct") or 0) for f in features) / max(len(features), 1)

        # Compute trend from current/prior when provided; otherwise fallback to trend field.
        trend_vals = []
        for f in features:
            current = float(f.get("score") or f.get("utilization") or f.get("adoption_pct") or 0)
            prior = float(f.get("prior_score") or f.get("prior_utilization") or 0)
            if prior:
                trend_vals.append(((current - prior) / prior * 100))
            else:
                trend_vals.append(float(f.get("trend") or 0))
        avg_trend = sum(trend_vals) / max(len(trend_vals), 1)

        if avg_adoption > 20:
            bg, fg = GREEN_BG, GREEN_FG
            threshold = ("Above", bg, fg)
        elif avg_adoption >= 6:
            bg, fg = YELLOW_BG, YELLOW_FG
            threshold = ("Watch", bg, fg)
        else:
            bg, fg = RED_BG, RED_FG
            threshold = ("Below", bg, fg)

        if avg_trend > 2:
            trend = (f"↑ +{avg_trend:.0f}%", GREEN_BG, GREEN_FG)
        elif avg_trend < -2:
            trend = (f"↓ {avg_trend:.0f}%", RED_BG, RED_FG)
        else:
            trend = ("→ Stable", GRAY_BG, GRAY_FG)

        if avg_adoption > 20:
            usage = (f"{avg_adoption:.0f}%", GREEN_BG, GREEN_FG)
        elif avg_adoption >= 6:
            usage = (f"{avg_adoption:.0f}%", YELLOW_BG, YELLOW_FG)
        else:
            usage = (f"{avg_adoption:.0f}%", RED_BG, RED_FG)

        rows.append({
            "group": group_name,
            "usage": usage,
            "threshold": threshold,
            "trend": trend,
        })

    rows.sort(key=lambda x: float(str(x["usage"][0]).replace("%", "")), reverse=True)

    logger.warning(f"TOTAL GROUPS: {len(rows)}")
    logger.warning(f"GROUP NAMES: {[r['group'] for r in rows]}")

    def truncate(name: str, max_chars: int = 16) -> str:
        return name if len(name) <= max_chars else name[: max_chars - 1] + "…"

    n_cols = 4
    fig, ax = plt.subplots(figsize=(12, 26))  # 1200×2600px @ 100 dpi
    fig.patch.set_facecolor("#FAFBFC")
    ax.set_facecolor("#FAFBFC")

    cell_w = 0.23
    cell_h = 0.13
    x_start = 0.02
    x_gap = 0.015
    y_start = 0.90

    for idx, row in enumerate(rows):
        col = idx % n_cols
        row_num = idx // n_cols

        x = x_start + col * (cell_w + x_gap)
        y = y_start - row_num * (cell_h + 0.01)

        bg = row["usage"][1]

        tile = FancyBboxPatch(
            (x, y - cell_h),
            cell_w,
            cell_h,
            boxstyle="round,pad=0.015",
            transform=ax.transAxes,
            facecolor=bg,
            edgecolor="#FFFFFF",
            linewidth=3,
            zorder=2,
        )
        ax.add_patch(tile)

        trend_text = str(row["trend"][0])
        trend_clean = (
            trend_text.replace("↑", "")
            .replace("↓", "")
            .replace("→", "")
            .replace("%", "")
            .replace("+", "")
            .replace("Stable", "0")
            .strip()
        )
        trend_val = float(trend_clean or 0)
        arrow = "↑" if trend_val > 2 else "↓" if trend_val < -2 else "→"

        ax.text(
            x + cell_w / 2,
            y - cell_h * 0.35,
            truncate(str(row["group"])),
            transform=ax.transAxes,
            fontsize=18,
            fontweight="bold",
            color="#1A1D21",
            ha="center",
            va="center",
            zorder=3,
        )

        ax.text(
            x + cell_w / 2,
            y - cell_h * 0.72,
            f"{row['usage'][0]}  {arrow}",
            transform=ax.transAxes,
            fontsize=26,
            fontweight="bold",
            color="#1A1D21",
            ha="center",
            va="center",
            zorder=3,
        )

    ax.text(
        0.02,
        0.97,
        "Adoption Heatmap",
        transform=ax.transAxes,
        fontsize=32,
        fontweight="bold",
        color="#1A1D21",
        va="top",
        zorder=5,
    )
    ax.text(
        0.98,
        0.97,
        _heatmap_subtitle(cloud, fy, len(rows)),
        transform=ax.transAxes,
        fontsize=14,
        color="#999999",
        ha="right",
        va="top",
        zorder=5,
    )

    ax.axis("off")
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)

    plt.subplots_adjust(left=0.01, right=0.99, top=0.99, bottom=0.01)
    buf = BytesIO()
    plt.savefig(buf, format="png", dpi=100, bbox_inches=None, facecolor="#FAFBFC")
    plt.close()
    buf.seek(0)
    return buf.read()


def build_heatmap_png(data: dict) -> bytes:
    """
    Backward-compatible wrapper for existing callsites.
    """
    cloud_family = str(data.get("cloud_family") or "")
    thresholds = get_thresholds(cloud_family)
    cloud = str(
        data.get("cloud")
        or (str(data.get("title") or "").replace(" Adoption Heatmap", "").strip() or "Adoption")
    )
    fy = str(data.get("fy") or "")
    return generate_heatmap_image(
        scored_data=list(data.get("features") or []),
        cloud=cloud,
        fy=fy,
        thresholds=thresholds,
    )


def upload_heatmap_and_get_private_url(
    client,
    scored_data: list[dict],
    cloud: str,
    fy: str,
    thresholds: dict = None,
    user_id: str = "",
) -> tuple[str, str]:
    png_bytes = generate_heatmap_image(
        scored_data, cloud, fy, thresholds
    )
    filename = f"heatmap_{cloud.replace(' ', '_')}_{fy}.png"

    dm = client.conversations_open(users=user_id)
    channel_id = dm["channel"]["id"]

    upload = client.files_upload_v2(
        content=png_bytes,
        filename=filename,
        channel=channel_id,
    )
    file_obj = upload["file"]
    all_urls = {
        k: v for k, v in file_obj.items()
        if any(x in k.lower() for x in ["url", "thumb", "permalink", "link"])
    }
    logger.debug("Heatmap file URL fields: %s", all_urls)
    url = (
        file_obj.get("permalink")
        or file_obj.get("url_private_download")
        or file_obj.get("url_private")
    )
    return url, file_obj["id"]


def get_or_refresh_heatmap_image(
    client, scored_data, cloud, fy, thresholds=None, user_id=""
) -> str | None:
    """
    Upload (or return cached) PNG and return the Slack **file id** (F...).
    App Home must use a Block Kit image block with `slack_file: {id}` — not
    `image_url` to slack-files.com (views.publish returns invalid_slack_file).
    """
    from datetime import datetime

    now = datetime.utcnow()
    cache = HEATMAP_IMAGE_CACHE
    ttl_sec = 6 * 3600

    if (
        cache.get("file_id")
        and cache.get("cloud") == cloud
        and cache.get("fy") == fy
        and cache.get("generated_at")
        and (now - cache["generated_at"]).total_seconds() < ttl_sec
    ):
        return cache["file_id"]

    if cache.get("file_id"):
        try:
            client.files_delete(file=cache["file_id"])
        except Exception:
            pass

    _, file_id = upload_heatmap_and_get_private_url(
        client, scored_data, cloud, fy, thresholds, user_id
    )
    if not file_id:
        return None

    HEATMAP_IMAGE_CACHE.update({
        "file_id": file_id,
        "generated_at": now,
        "cloud": cloud,
        "fy": fy,
    })

    return file_id
