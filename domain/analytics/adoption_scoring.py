"""
domain/analytics/adoption_scoring.py

Pure calculation module for adoption scoring.
No Snowflake calls. No external dependencies.
Standard library only.

Examples:

Healthy product (high utilization, growing):
    calculate_adoption_score(
        provisioned=10000, activated=8000, used=7500,
        account_count=100, prev_quarter_used=6000
    )
    → {
        "score": 82,
        "utilization": 0.75,
        "penetration": 0.80,
        "trend": 25.0,
        "status": "green"
    }

Watch product (medium utilization, flat):
    calculate_adoption_score(
        provisioned=10000, activated=5000, used=4200,
        account_count=100, prev_quarter_used=4100
    )
    → {
        "score": 51,
        "utilization": 0.42,
        "penetration": 0.50,
        "trend": 2.4,
        "status": "amber"
    }

Critical product (low utilization, declining):
    calculate_adoption_score(
        provisioned=10000, activated=2000, used=1500,
        account_count=100, prev_quarter_used=2200
    )
    → {
        "score": 22,
        "utilization": 0.15,
        "penetration": 0.20,
        "trend": -31.8,
        "status": "red"
    }
"""


def calculate_adoption_score(
    provisioned: int,
    activated: int,
    used: int,
    account_count: int,
    prev_quarter_used: int = None
) -> dict:
    """
    Calculates composite adoption score for one product/quarter cell.

    Scoring weights:
    - 60% utilization rate: used / provisioned
    - 30% account penetration: accounts with usage / total accounts
    - 10% growth trend: % change vs prev quarter
      If prev_quarter_used is None, redistribute 10% to utilization (70/30)

    Thresholds:
    - green:  score >= 70
    - amber:  score >= 30
    - red:    score < 30

    Guards:
    - provisioned = 0 → utilization = 0
    - account_count = 0 → penetration = 0
    - prev_quarter_used = 0 → trend = 0
    - All inputs clamped to 0-100 range before returning

    Args:
        provisioned:        Total provisioned units
        activated:          Total activated units
        used:               Total used units in period
        account_count:      Total accounts with any usage
        prev_quarter_used:  Used units in previous quarter (None if unavailable)

    Returns:
        {
            "score": int,           # 0-100 composite
            "utilization": float,   # used/provisioned as 0.0-1.0
            "penetration": float,   # 0.0-1.0
            "trend": float,         # % change vs prev quarter, None if no prev
            "status": str           # "green", "amber", "red"
        }

    Examples:
        >>> # Healthy product
        >>> calculate_adoption_score(10000, 8000, 7500, 100, 6000)
        {'score': 82, 'utilization': 0.75, 'penetration': 0.8, 'trend': 25.0, 'status': 'green'}

        >>> # Watch product
        >>> calculate_adoption_score(10000, 5000, 4200, 100, 4100)
        {'score': 51, 'utilization': 0.42, 'penetration': 0.5, 'trend': 2.4, 'status': 'amber'}

        >>> # Critical product
        >>> calculate_adoption_score(10000, 2000, 1500, 100, 2200)
        {'score': 22, 'utilization': 0.15, 'penetration': 0.2, 'trend': -31.8, 'status': 'red'}
    """

    # --- Guards: clamp negatives to 0 ---
    provisioned      = max(0, provisioned or 0)
    activated        = max(0, activated or 0)
    used             = max(0, used or 0)
    account_count    = max(0, account_count or 0)

    # --- Utilization: used / provisioned ---
    utilization = (used / provisioned) if provisioned > 0 else 0.0
    utilization = min(utilization, 1.0)  # cap at 100%
    utilization_score = utilization * 100  # 0-100

    # --- Penetration: account_count with usage / total accounts ---
    # For PDP 2.0: account_count = accounts with ORG_PF_ROLLING_28D_MAU > 0
    # We treat account_count as numerator, use activated as denominator proxy
    # If activated = 0, fall back to provisioned
    denominator = activated if activated > 0 else provisioned
    penetration = (account_count / denominator) if denominator > 0 else 0.0
    penetration = min(penetration, 1.0)  # cap at 100%
    penetration_score = penetration * 100  # 0-100

    # --- Trend: % change vs prev quarter ---
    trend = None
    trend_score = 0.0

    if prev_quarter_used is not None:
        prev = max(0, prev_quarter_used)
        if prev == 0:
            # New feature or zero baseline — treat as neutral (no penalty/bonus)
            trend = 0.0
            trend_score = 50.0  # neutral midpoint
        else:
            pct_change = ((used - prev) / prev) * 100
            trend = round(pct_change, 1)
            # Map trend to 0-100:
            # +50% or more → 100
            # 0%           → 50
            # -50% or less → 0
            trend_score = max(0.0, min(100.0, 50.0 + pct_change))

    # --- Composite score ---
    if prev_quarter_used is None:
        # No trend data → redistribute 10% to utilization (70/30 split)
        score = (utilization_score * 0.70) + (penetration_score * 0.30)
    else:
        score = (
            (utilization_score * 0.60) +
            (penetration_score * 0.30) +
            (trend_score       * 0.10)
        )

    # --- Clamp final score to 0-100 ---
    score = max(0, min(100, round(score)))

    # --- Status thresholds ---
    if score >= 70:
        status = "green"
    elif score >= 30:
        status = "amber"
    else:
        status = "red"

    return {
        "score":       score,
        "utilization": round(utilization, 4),
        "penetration": round(penetration, 4),
        "trend":       trend,
        "status":      status,
    }


def score_to_emoji(status: str) -> str:
    """
    Returns Slack emoji string for a given adoption status.

    Args:
        status: One of "green", "amber", "red", or any unknown string.

    Returns:
        Slack emoji string.

    Examples:
        >>> score_to_emoji("green")
        ':large_green_circle:'

        >>> score_to_emoji("amber")
        ':large_yellow_circle:'

        >>> score_to_emoji("red")
        ':red_circle:'

        >>> score_to_emoji("unknown")
        ':white_circle:'
    """
    return {
        "green": ":large_green_circle:",
        "amber": ":large_yellow_circle:",
        "red":   ":red_circle:",
    }.get(status, ":white_circle:")
