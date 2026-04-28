import json
import logging

from domain.analytics.heatmap_queries import (
    _CLOUD_MAPPING,
    get_adoption_heatmap_data,
)
from domain.content.heatmap_builder import (
    build_adoption_heatmap_blocks,
    build_adoption_heatmap_canvas,
)
from log_utils import log_debug

logger = logging.getLogger(__name__)


def get_available_clouds() -> list[str]:
    """Returns supported cloud names for the heatmap slash command."""
    return list(_CLOUD_MAPPING.keys())


def run_adoption_heatmap(
    cloud: str,
    fy: str = "FY2027",
    industry: str | None = None,
    region: str | None = None,
) -> str:
    """
    Orchestrates the full adoption heatmap flow.
    Returns a Slack Canvas markdown string ready to post.

    Flow:
    1. get_adoption_heatmap_data(cloud, fy, industry, region)
       → already scores each feature internally
       → returns { quarters: { Q1: [...], Q2: [...] }, summary: {...} }
    2. Pick most recent quarter with data
    3. build_adoption_heatmap_blocks(...) with filters (Block Kit parity)
    4. build_adoption_heatmap_canvas(features, cloud, fy, industry, region)
    5. Return canvas markdown string

    Raises:
    - ValueError: if cloud not in _CLOUD_MAPPING
    - RuntimeError: if data fetch or canvas build fails
    """

    logger.info(f"Fetching heatmap data for {cloud} {fy}")

    # Step 1 — Fetch + score (scoring happens inside get_adoption_heatmap_data)
    try:
        result = get_adoption_heatmap_data(
            cloud,
            fy,
            industry=industry,
            region=region,
        )
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
        logger.warning(
            f"No data for {cloud} {fy} (industry={industry}, region={region}) — "
            "returning empty canvas"
        )
        return f"# {cloud} · Adoption Heatmap · {fy}\n\n_No data available._"

    # Log score summary
    summary = result.get("summary", {})
    logger.info(
        f"Scored {len(features)} features: "
        f"{summary.get('green', 0)} green, "
        f"{summary.get('amber', 0)} amber, "
        f"{summary.get('red', 0)} red"
    )

    # Step 3 — Block Kit (same data path; Slack also builds blocks in-app)
    build_adoption_heatmap_blocks(
        features, cloud, fy, industry=industry, region=region,
    )

    # Step 4 — Canvas markdown
    try:
        canvas = build_adoption_heatmap_canvas(
            features, cloud, fy, industry=industry, region=region,
        )
    except Exception as e:
        logger.error(f"Canvas generation failed: {e}")
        raise RuntimeError(f"Canvas generation failed: {e}")

    logger.info(f"Canvas generated: {len(canvas)} characters")
    return canvas


def classify_adoption_intent(text: str, call_llm_fn=None) -> dict:
    """
    Sends user message to LLM gateway and returns a structured intent dict.

    Args:
        text: User's natural language message
        call_llm_fn: Optional LLM gateway function (from server.py)

    Returns:
    {
        "type": str,         # see intent types below
        "cloud": str,        # "Commerce B2B" or null
        "fy": str,           # "FY2027" or null
        "feature_group": str, # group name or null
        "feature": str,      # feature name or null
        "account": str,      # account name or null
        "industry": str,     # industry or null
        "region": str        # region or null
    }

    Intent types:
    "heatmap_summary"  - overall adoption view
    "group_drilldown"  - specific feature group
    "feature_detail"   - specific feature
    "account_lookup"   - specific account/org
    "top_movers"       - who is growing/declining
    "feature_owner"    - who owns a feature
    "industry_filter"  - filter by industry
    "region_filter"    - filter by region
    "not_adoption"     - not an adoption query
    """

    # Known feature groups (from heatmap data)
    known_groups = [
        "Markets/I18n", "Buyer Groups", "Pricing", "Search",
        "Setup & User Tools", "Cart", "Shipping",
        "Product & Catalog", "B2B Payments",
        "Shopper Experience & Profiles", "Promotions",
        "Checkout", "Import/Export Tools", "Payments",
        "Agentforce for Shopping", "Analytics",
        "Buyer Messaging", "Data Cloud for Commerce",
        "Subscriptions", "Tax"
    ]

    prompt = f"""You are an intent classifier for a Salesforce Commerce B2B adoption analytics bot.

Known feature groups:
{", ".join(known_groups)}

Given a user message, return ONLY a JSON object.
No explanation. No markdown. Just JSON.

{{
  "type": "heatmap_summary|group_drilldown|feature_detail|account_lookup|top_movers|feature_owner|industry_filter|region_filter|not_adoption",
  "cloud": "Commerce B2B or null",
  "fy": "FY2027 or null",
  "feature_group": "exact group name or null",
  "feature": "feature name or null",
  "account": "account name or null",
  "industry": "industry name or null",
  "region": "region name or null"
}}

User message: "{text}"
"""

    system_prompt = (
        "You are a precise intent classifier. Return only valid JSON. "
        "Use null for missing fields. Match feature_group names exactly from the provided list."
    )

    # Try LLM first if available
    if call_llm_fn:
        try:
            log_debug(f"Calling LLM for intent classification: {text[:50]}")
            raw_response = call_llm_fn(prompt, system_prompt=system_prompt, max_tokens=300)

            if raw_response:
                # Clean response (remove markdown if present)
                cleaned = raw_response.strip()
                if cleaned.startswith("```"):
                    # Remove markdown code blocks
                    lines = cleaned.split("\n")
                    cleaned = "\n".join([l for l in lines if not l.startswith("```")])
                    cleaned = cleaned.strip()

                try:
                    intent = json.loads(cleaned)
                    log_debug(f"LLM intent classification: {intent.get('type')}")
                    return intent
                except json.JSONDecodeError as e:
                    log_debug(f"LLM response was not valid JSON: {str(e)[:100]}")
        except Exception as e:
            log_debug(f"LLM intent classification error: {str(e)[:100]}")

    # Fallback to keyword matching
    log_debug("Using keyword fallback for intent classification")
    text_lower = text.lower()

    fallback = {
        "type": "not_adoption",
        "cloud": "Commerce B2B",
        "fy": "FY2027",
        "feature_group": None,
        "feature": None,
        "account": None,
        "industry": None,
        "region": None
    }

    # Check for feature group mentions
    for group in known_groups:
        if group.lower() in text_lower:
            fallback["type"] = "group_drilldown"
            fallback["feature_group"] = group
            return fallback

    # Check for common group name variations
    group_variations = {
        "cart": "Cart",
        "checkout": "Checkout",
        "pricing": "Pricing",
        "search": "Search",
        "shipping": "Shipping",
        "payments": "Payments",
        "b2b payments": "B2B Payments",
        "analytics": "Analytics",
        "promotions": "Promotions",
        "subscriptions": "Subscriptions",
        "tax": "Tax",
        "buyer groups": "Buyer Groups",
        "buyer messaging": "Buyer Messaging",
        "markets": "Markets/I18n",
        "agentforce": "Agentforce for Shopping",
    }

    for variation, canonical in group_variations.items():
        if variation in text_lower:
            fallback["type"] = "group_drilldown"
            fallback["feature_group"] = canonical
            return fallback

    # Check for summary/heatmap requests
    if any(w in text_lower for w in
           ["summary", "heatmap", "overview", "adoption", "how is", "how are"]):
        fallback["type"] = "heatmap_summary"
        return fallback

    # Check for top movers
    if any(w in text_lower for w in
           ["mover", "growing", "declining", "top", "worst", "best", "gainers", "losers"]):
        fallback["type"] = "top_movers"
        return fallback

    # Check for owner queries
    if any(w in text_lower for w in
           ["who owns", "owner", "pm for", "who is responsible"]):
        fallback["type"] = "feature_owner"
        return fallback

    return fallback
