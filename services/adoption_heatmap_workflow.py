import logging

from domain.analytics.heatmap_queries import (
    _CLOUD_MAPPING,
    get_adoption_heatmap_data,
)
from domain.content.heatmap_builder import build_adoption_heatmap_canvas

logger = logging.getLogger(__name__)


def get_available_clouds() -> list[str]:
    """Returns supported cloud names for the heatmap slash command."""
    return list(_CLOUD_MAPPING.keys())


def run_adoption_heatmap(
    cloud: str,
    fy: str = "FY2027",
) -> str:
    """
    Orchestrates the full adoption heatmap flow.
    Returns a Slack Canvas markdown string ready to post.

    Flow:
    1. get_adoption_heatmap_data(cloud, fy)
       → already scores each feature internally
       → returns { quarters: { Q1: [...], Q2: [...] }, summary: {...} }
    2. Pick most recent quarter with data
    3. build_adoption_heatmap_canvas(features, cloud, fy)
    4. Return canvas markdown string

    Raises:
    - ValueError: if cloud not in _CLOUD_MAPPING
    - RuntimeError: if data fetch or canvas build fails
    """

    logger.info(f"Fetching heatmap data for {cloud} {fy}")

    # Step 1 — Fetch + score (scoring happens inside get_adoption_heatmap_data)
    try:
        result = get_adoption_heatmap_data(cloud, fy)
    except ValueError:
        raise  # let caller handle unknown cloud
    except Exception as e:
        logger.error(f"Heatmap data fetch failed: {e}")
        raise RuntimeError(f"Heatmap data fetch failed for {cloud} {fy}: {e}")

    # Step 2 — Pick most recent quarter with data
    quarters = result.get("quarters", {})
    features = []
    latest_quarter = None
    for q in ["Q4", "Q3", "Q2", "Q1"]:  # newest first
        if quarters.get(q):
            features = quarters[q]
            latest_quarter = q
            break

    n_products = len({f["feature"] for f in features})
    logger.info(
        f"Retrieved {len(features)} features for {n_products} products ({latest_quarter})"
    )

    if not features:
        logger.warning(f"No data for {cloud} {fy} — returning empty canvas")
        return f"# {cloud} · Adoption Heatmap · {fy}\n\n_No data available._"

    # Log score summary
    summary = result.get("summary", {})
    logger.info(
        f"Scored {len(features)} features: "
        f"{summary.get('green', 0)} green, "
        f"{summary.get('amber', 0)} amber, "
        f"{summary.get('red', 0)} red"
    )

    # Step 3 — Build canvas
    try:
        canvas = build_adoption_heatmap_canvas(features, cloud, fy)
    except Exception as e:
        logger.error(f"Canvas generation failed: {e}")
        raise RuntimeError(f"Canvas generation failed: {e}")

    logger.info(f"Canvas generated: {len(canvas)} characters")
    return canvas
