"""
filter_parser.py
Unified filter parser for /gm-review-canvas, /at-risk-canvas,
/attrition-risk, and handle_message NL routing
"""
import re

# ── Constants ─────────────────────────────────────────────────────────────

CLOUD_KEYWORDS = [
    "B2C Commerce", "B2B Commerce", "Commerce Cloud",
    "Financial Services Cloud",
    "FSC",
    "Marketing Cloud", "Sales Cloud", "Service Cloud",
    "Data Cloud", "MuleSoft", "Tableau", "Agentforce",
    "Slack", "Order Management", "All Clouds",
]

FSC_KEYWORDS = frozenset(
    {
        "fsc",
        "financial services",
        "financial services cloud",
        "financial cloud",
        "wealth management",
        "insurance",
        "banking cloud",
        "lending",
    }
)

REGION_MAP = {
    "emea": "EMEA", "amer": "AMER",
    "apac": "APAC", "jp": "JP",
}

FY_MAP = {
    "fy27": "FY2027", "fy2027": "FY2027",
    "fy26": "FY2026", "fy2026": "FY2026",
}

QUARTER_MAP = {
    "q1": "Q1", "q2": "Q2",
    "q3": "Q3", "q4": "Q4",
}

ARI_MAP = {
    "ari:high":   "High",
    "high risk":  "High",
    "ari:medium": "Medium",
    "med risk":   "Medium",
    "ari:low":    "Low",
    "low risk":   "Low",
}

HEALTH_MAP = {
    "health:red":    "Red",
    "red only":      "Red",
    "health:yellow": "Yellow",
    "yellow only":   "Yellow",
    "health:green":  "Green",
}

SORT_MAP = {
    "sort:atr":    "atr",
    "sort:aov":    "cc_aov",
    "sort:close":  "close_date",
    "sort:ari":    "ari",
    "sort:health": "health",
    "sort:gmv":    "gmv_rate",
}

FILTER_WORDS = (
    list(REGION_MAP.keys()) +
    list(FY_MAP.keys()) +
    list(QUARTER_MAP.keys()) +
    [">1m", ">500k", ">400k", ">200k",
     ">aov1m", ">aov500k", ">aov400k",
     "for", "with", "top", "closing",
     "ari:high", "ari:medium", "ari:low",
     "high risk", "med risk", "low risk",
     "health:red", "health:yellow", "health:green",
     "red only", "yellow only",
     "sort:atr", "sort:aov", "sort:close",
     "sort:ari", "sort:health", "sort:gmv",
     ]
)


# ── Main Parser ───────────────────────────────────────────────────────────

def parse_filters(text: str) -> dict:
    """
    Universal filter parser.
    Works for /gm-review-canvas, /at-risk-canvas,
    /attrition-risk and NL handle_message routing.
    """
    t = text.lower().strip()

    # Cloud — FSC / Financial Services before generic keyword scan
    cloud = "Commerce Cloud"
    cloud_explicit = False
    if any(kw in t for kw in FSC_KEYWORDS):
        cloud = "Financial Services Cloud"
        cloud_explicit = True
    else:
        for kw in CLOUD_KEYWORDS:
            if kw.lower() in t:
                cloud = kw
                cloud_explicit = True
                break

    # Region
    region = next((v for k, v in REGION_MAP.items() if k in t), None)

    # FY
    fy = next((v for k, v in FY_MAP.items() if k in t), None)

    # Quarter
    quarter = next((v for k, v in QUARTER_MAP.items() if k in t), None)

    # ATR threshold — highest wins if multiple
    if ">1m" in t:
        min_attrition, attrition_label = 1000000, ">$1M"
    elif ">500k" in t:
        min_attrition, attrition_label = 500000, ">$500K"
    elif ">400k" in t:
        min_attrition, attrition_label = 400000, ">$400K"
    elif ">200k" in t:
        min_attrition, attrition_label = 200000, ">$200K"
    else:
        min_attrition, attrition_label = 500000, ">$500K"

    # CC AOV threshold — highest wins
    if ">aov1m" in t:
        min_aov = 1000000
    elif ">aov500k" in t:
        min_aov = 500000
    elif ">aov400k" in t:
        min_aov = 400000
    else:
        min_aov = 0

    # ARI filter
    ari_filter = next((v for k, v in ARI_MAP.items() if k in t), None)

    # Health filter
    health_filter = next((v for k, v in HEALTH_MAP.items() if k in t), None)

    # Limit — top N
    m = re.search(r"top\s*(\d+)", t)
    limit = int(m.group(1)) if m else 10

    # Close filter — "closing march", "closing Q1", "closing this month"
    m = re.search(r"closing\s+(\w+(?:\s+\w+)?)", t)
    close_filter = m.group(1).strip() if m else None

    # Sort
    sort_by = next((v for k, v in SORT_MAP.items() if k in t), "atr")

    # Is manual (account name list)?
    SUFFIX_PATTERN = (
        r",\s*(?="
        r"(?!Inc\b|LLC\b|Ltd\b|GmbH\b|Corp\b|Co\b"
        r"|B\.V\b|S\.A\b|A\.S\b|S\.L\b|Pte\b|Pty\b"
        r"|A/S\b|a/s\b|SpA\b|SRL\b|S\.r\.l\b|KG\b"
        r"|AG\b|AB\b|AS\b|OY\b|NV\b|PLC\b|plc\b"
        r"|S\.p\.A\b|N\.V\b"
        r"))"
    )
    parts = [p.strip() for p in re.split(SUFFIX_PATTERN, text)]

    def _segment_is_account_only(seg: str) -> bool:
        """False if this segment clearly contains canvas filter tokens (not a bare account name)."""
        s = seg.strip().lower()
        if not s:
            return False
        if re.search(r"\btop\s+\d+\b", s):
            return False
        if any(tok in s for tok in (">1m", ">500k", ">400k", ">200k",
                                    ">aov1m", ">aov500k", ">aov400k")):
            return False
        if re.search(
            r"\b(" + "|".join(re.escape(k) for k in REGION_MAP) + r")\b", s
        ):
            return False
        if re.search(
            r"\b(" + "|".join(re.escape(k) for k in FY_MAP) + r")\b", s
        ):
            return False
        if re.search(
            r"\b(" + "|".join(re.escape(k) for k in QUARTER_MAP) + r")\b", s
        ):
            return False
        for tok in s.split():
            if tok in FILTER_WORDS:
                return False
            if tok.startswith(">") or tok.startswith("fy") or tok.startswith("q"):
                return False
            if tok.startswith("sort:"):
                return False
            if tok.startswith("ari:") or tok.startswith("health:"):
                return False
        return True

    cloud_lower = {kw.lower() for kw in CLOUD_KEYWORDS}
    account_candidate_parts = parts
    if len(parts) >= 2 and parts[0].strip().lower() in cloud_lower:
        account_candidate_parts = parts[1:]

    is_manual = len(account_candidate_parts) >= 1 and all(
        _segment_is_account_only(p) for p in account_candidate_parts
    )

    manual_account_parts = (
        account_candidate_parts if is_manual else []
    )

    # Opp IDs (explicit tokens, 15-18 chars; keep SF opportunity prefix 006)
    opp_ids: list[str] = []
    for tok in re.split(r"[\s,]+", text):
        t_tok = tok.strip()
        if not t_tok:
            continue
        if re.match(r"^[0-9A-Za-z]{15,18}$", t_tok) and t_tok.startswith("006"):
            opp_ids.append(t_tok)
    opp_ids = list(dict.fromkeys(opp_ids))

    return {
        "cloud":                 cloud,
        "cloud_explicit":        cloud_explicit,
        "region":                region,
        "fy":                    fy,
        "quarter":               quarter,
        "min_attrition":         min_attrition,
        "attrition_label":       attrition_label,
        "min_aov":               min_aov,
        "ari_filter":            ari_filter,
        "health_filter":         health_filter,
        "limit":                 limit,
        "close_filter":          close_filter,
        "sort_by":               sort_by,
        "is_manual":             is_manual,
        "opp_ids":               opp_ids,
        "parts":                 parts,
        "manual_account_parts":  manual_account_parts,
    }


# ── Filter Label Builder ──────────────────────────────────────────────────

def build_filter_label(f: dict) -> str:
    """Human-readable filter summary for say() messages."""
    label = f["cloud"]
    if f["region"]:
        label += " - " + f["region"]
    if f["quarter"]:
        label += " - " + f["quarter"] + " " + f["fy"]
    else:
        label += " - " + f["fy"]
    label += " - " + f["attrition_label"]
    if f["min_aov"]:
        label += " - AOV>" + ("$1M" if f["min_aov"] >= 1000000 else "$500K")
    if f["ari_filter"]:
        label += " - ARI:" + f["ari_filter"]
    if f["health_filter"]:
        label += " - Health:" + f["health_filter"]
    if f["close_filter"]:
        label += " - Closing:" + f["close_filter"]
    if f["limit"] != 10:
        label += " - Top " + str(f["limit"])
    return label
