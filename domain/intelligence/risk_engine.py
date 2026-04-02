"""
risk_engine.py
Handles risk classification and AI analysis.
"""
import os
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()

# ================================================
# RISK MAPPING
# ================================================

RISK_RECOMMENDATION_MAP = {
    "platform_underutilization": {
        "theme": "Platform Underutilization",
        "description": "Customer is not using all their purchased GMV.",
        "indicators": [
            "underutilization", "low utilization", "unused gmv",
            "burn rate", "underconsumption", "low usage",
            "shelfware", "low adoption",
        ],
        "risk_reasons": ["Adoption", "Financial & Contractual"],
        "examples": [
            "Wedgewood Village Pharmacy", "The Warehouse Group",
            "Floor & Decor Holdings", "Orvis Company",
        ],
        "recommendations": [
            "Involve Consumption Leads to validate metrics and address underutilization",
            "AOVPP Swaps: Move unused GMV into Agentforce, Retail Cloud, or Marketing Cloud",
            "Right-size the contract at renewal",
        ],
    },
    "competitive_threat": {
        "theme": "Competitive Threat & High TCO",
        "description": "Active RFPs with Shopify or CommerceTools.",
        "indicators": [
            "shopify", "commercetools", "competitor", "rfp",
            "too expensive", "tco", "competitive", "migrating",
        ],
        "risk_reasons": ["Competitive", "Financial & Contractual"],
        "examples": [
            "Duluth Holding", "Cartier", "Suit Supply", "SMCP Group",
        ],
        "recommendations": [
            "Executive Outreach: B2C roadmap sessions showcasing Agentic Commerce",
            "Commercial Leverage: Multi-cloud deals, revisit GMV pricing, AOVPP restructure",
        ],
    },
    "implementation_failure": {
        "theme": "Implementation Failures & Tech Debt",
        "description": "Delayed go-lives or performance issues.",
        "indicators": [
            "implementation", "delayed", "go-live", "performance",
            "instability", "tech debt", "architecture", "overdue",
        ],
        "risk_reasons": ["Implementation", "Product/Technology"],
        "examples": [
            "ASDA", "AWWG (Pepe Jeans)",
            "Allied Beverage Group", "Canada Federal Sciences",
        ],
        "recommendations": [
            "Deploy Expertise: Assign Success Architects or ProServ CTO support",
            "Engineering Engagement: Resolve performance/scale issues",
        ],
    },
    "feature_gaps": {
        "theme": "Feature Gaps & Legacy Tech",
        "description": "Customer needs composable/MACH or has feature gaps.",
        "indicators": [
            "feature gap", "composable", "mach", "headless",
            "search", "missing feature", "legacy", "outdated",
        ],
        "risk_reasons": ["Product/Technology", "Product"],
        "examples": [
            "Vishal Mega Mart", "ETAM Developpement", "Ildico Watches",
        ],
        "recommendations": [
            "Targeted Workshops: Demonstrate composable capabilities vs competitors",
            "Beta/Pilot Programs: Highlight Merchant Agent for semantic search",
        ],
    },
    "business_shift": {
        "theme": "Business/Leadership Shifts",
        "description": "New CIO, M&A, or industry downturns.",
        "indicators": [
            "new cio", "new leadership", "m&a", "merger",
            "downsizing", "restructuring", "budget cut",
        ],
        "risk_reasons": [
            "Business Change & Distress",
            "Merger & Acquisition",
        ],
        "examples": ["Cotton On", "Sparc Group", "GoPro"],
        "recommendations": [
            "Bridge Renewals: Secure 10-16 month short-term renewals",
            "Strategic Repositioning: Transform escalations into consolidation deals",
        ],
    },
}

# Actionable risk reasons
ACTIONABLE_REASONS = {
    "Adoption",
    "Implementation",
    "Product/Technology",
    "Product",
    "Success Plan & Support",
    "Customer Relationship",
    "Financial & Contractual",
    "Pricing/Contract",
    "Economic",
    "Oversold",
    "Low Perceived ROI",
    "No Reason Given",
    "No Given Reason",
}

# Non-actionable risk reasons
NON_ACTIONABLE_REASONS = {
    "Competitive",
    "Business Change & Distress",
    "Merger & Acquisition",
    "Policy & Compliance",
    "Downsizing / Reduce Spend",
}


# ================================================
# CLASSIFICATION
# ================================================

def is_actionable(risk_reason: str) -> bool:
    """Check if risk reason is actionable."""
    if not risk_reason:
        return True
    for reason in NON_ACTIONABLE_REASONS:
        if reason.lower() in risk_reason.lower():
            return False
    return True


def classify_risk(
    risk_reason: str,
    risk_detail: str = "",
    description: str = "",
    utilization: str = "",
) -> dict:
    """
    Classify risk situation and return theme + recommendations.
    """
    all_text = (
        str(risk_reason) + " " +
        str(risk_detail) + " " +
        str(description)
    ).lower()

    # Check utilization for underutilization signal
    try:
        util_val = float(str(utilization).replace("%", "").strip())
        if util_val < 50:
            all_text += " low utilization underconsumption"
    except Exception:
        pass

    # Score each category
    scores = {}
    for key, mapping in RISK_RECOMMENDATION_MAP.items():
        score = 0
        for indicator in mapping["indicators"]:
            if indicator in all_text:
                score += 2
        for reason in mapping["risk_reasons"]:
            if reason.lower() in str(risk_reason).lower():
                score += 5
        scores[key] = score

    # Get best match
    if not scores or max(scores.values()) == 0:
        return {
            "theme": "Needs Review",
            "description": "Risk situation needs manual review",
            "recommendations": ["Review account details manually"],
            "confidence": "Low",
            "actionable": is_actionable(risk_reason),
        }

    best_key = max(scores, key=scores.get)
    best = RISK_RECOMMENDATION_MAP[best_key]
    confidence = (
        "High" if scores[best_key] >= 7 else
        "Medium" if scores[best_key] >= 3 else
        "Low"
    )

    return {
        "theme": best["theme"],
        "description": best["description"],
        "recommendations": best["recommendations"],
        "examples": best.get("examples", []),
        "confidence": confidence,
        "actionable": is_actionable(risk_reason),
    }


# ================================================
# AI RISK ANALYSIS
# ================================================

def generate_risk_analysis(
    account_name: str,
    opp: dict = None,
    red_account: dict = None,
    snowflake_enrichment: dict = None,
    call_llm_fn=None,
) -> tuple:
    """
    Generate VP-level risk analysis using Claude.
    Returns: (risk_notes, recommendations)
    """
    # Build context from all sources
    context_lines = ["Account: " + account_name]

    # org62 signals
    if opp:
        risk_reason = str(opp.get("License_At_Risk_Reason__c") or "N/A")
        risk_detail = str(opp.get("ACV_Reason_Detail__c") or "")
        forecasted = abs(opp.get("Forecasted_Attrition__c") or 0)
        swing = abs(opp.get("Swing__c") or 0)
        close_date = str(opp.get("CloseDate") or "N/A")
        next_steps = str(opp.get("NextStep") or "")
        description = str(opp.get("Description") or "")
        specialist = str(opp.get("Specialist_Sales_Notes__c") or "")

        context_lines += [
            f"Risk Reason: {risk_reason}",
            f"Risk Detail: {risk_detail}",
            f"Forecasted Attrition: ${forecasted:,.0f}",
            f"Swing: ${swing:,.0f}",
            f"Close Date: {close_date}",
        ]
        if next_steps:
            context_lines.append(f"Next Steps: {next_steps[:300]}")
        if description:
            context_lines.append(f"Description: {description[:300]}")
        if specialist:
            context_lines.append(f"CSG Notes: {specialist[:200]}")
    else:
        risk_reason = "N/A"
        risk_detail = ""
        description = ""

    # Red Account signals
    if red_account:
        acv = red_account.get("ACV_at_Risk__c")
        try:
            acv_num = float(acv) if acv is not None else 0.0
        except (TypeError, ValueError):
            acv_num = 0.0
        context_lines += [
            f"Red Account Stage: {red_account.get('Stage__c') or 'N/A'}",
            f"ACV at Risk: ${acv_num:,.0f}",
            f"Days Red: {red_account.get('Days_Red__c') or 0}",
            f"Trending: {red_account.get('Red_Trending__c') or 'N/A'}",
        ]

    # Snowflake signals
    if snowflake_enrichment:
        from domain.analytics.snowflake_client import format_enrichment_for_claude
        sf_context = format_enrichment_for_claude(snowflake_enrichment)
        if sf_context:
            context_lines.append(sf_context)

    # Classify risk
    utilization = ""
    if snowflake_enrichment:
        usage = snowflake_enrichment.get("usage", {})
        utilization = str(usage.get("utilization_rate") or "")

    risk_classification = classify_risk(
        risk_reason, risk_detail, description, utilization
    )

    context = "\n".join(context_lines)
    theme = risk_classification["theme"]
    recs = risk_classification["recommendations"]

    # Fallback if no LLM
    if not call_llm_fn:
        risk_notes = f"- {risk_reason}: {risk_detail}"
        recommendation = "\n".join(f"- {r}" for r in recs[:2])
        return risk_notes, recommendation

    # Generate risk notes via Claude
    try:
        risk_notes = call_llm_fn(
            f"Write 3-4 crisp bullet points about this account's attrition risk for a VP:\n"
            f"{context}\n\n"
            "Rules:\n"
            "- Start each bullet with -\n"
            "- NO headers or titles\n"
            "- NO markdown headers (#)\n"
            "- Be specific with numbers\n"
            "- Each bullet max 20 words\n"
            "- No filler words",
            system_prompt=(
                "Senior Salesforce Commerce Cloud PM. "
                "Output ONLY bullet points starting with -. "
                "No headers. No titles. No markdown. "
                "Max 4 bullets. Each max 20 words."
            ),
            max_tokens=200,
        )
    except Exception as e:
        print(f"Risk notes LLM error: {str(e)[:60]}")
        risk_notes = f"- {risk_reason}"
        if risk_detail:
            risk_notes += f": {risk_detail}"

    # Generate recommendations via Claude
    try:
        rec_context = (
            f"Account: {account_name}\n"
            f"Risk Theme: {theme}\n"
            f"Standard recommendations for this theme:\n" +
            "\n".join(f"- {r}" for r in recs) + "\n\n"
            f"Account context:\n{context[:400]}"
        )
        recommendation = call_llm_fn(
            f"Write 2-3 specific recommendations for this account:\n"
            f"{rec_context}\n\n"
            "Rules:\n"
            "- Start each with -\n"
            "- NO headers or numbered lists\n"
            "- NO markdown headers (#)\n"
            "- Tailor to THIS account specifically\n"
            "- Each recommendation max 20 words",
            system_prompt=(
                "Senior Salesforce Commerce Cloud PM. "
                "Output ONLY bullet points starting with -. "
                "No headers. No titles. No numbered lists. "
                "Max 3 bullets."
            ),
            max_tokens=150,
        )
    except Exception as e:
        print(f"Recommendation LLM error: {str(e)[:60]}")
        recommendation = "\n".join(f"- {r}" for r in recs[:2])

    return risk_notes.strip(), recommendation.strip()


class RiskEngine:
    """Structured risk + adoption analysis for workflow services."""

    def __init__(self, call_llm_fn=None):
        self.call_llm_fn = call_llm_fn

    def _enrichment_from_analytics(self, analytics: Dict[str, Any]) -> Dict[str, Any]:
        usage = analytics.get("usage") or {}
        ari = analytics.get("ari_score")
        attrition = analytics.get("attrition") or {}
        products = attrition.get("products") or []
        ari_scores = []
        for p in products[:15]:
            ari_scores.append({
                "product": p.get("product"),
                "category": p.get("category"),
                "probability": None,
                "reason": "",
            })
        if ari is not None:
            if not any(s.get("probability") for s in ari_scores):
                ari_scores.insert(0, {
                    "product": "Portfolio",
                    "category": "N/A",
                    "probability": float(ari),
                    "reason": "Snowflake aggregate ARI",
                })
        util_fmt = {}
        utilization = usage.get("utilization_rate")
        if utilization is not None:
            util_fmt["utilization_rate"] = (
                utilization if isinstance(utilization, str) else f"{float(utilization):.1f}%"
            )
        gmv = usage.get("gmv_rate")
        if gmv is not None:
            util_fmt["gmv_rate"] = gmv if isinstance(gmv, str) else f"{float(gmv):.1f}%"
        burn = usage.get("burn_rate")
        if burn is not None:
            util_fmt["burn_rate"] = burn if isinstance(burn, str) else f"{float(burn):.1f}%"
        cc_aov = usage.get("cc_aov")
        if cc_aov is not None:
            util_fmt["cc_aov"] = cc_aov if isinstance(cc_aov, str) else f"${float(cc_aov):,.0f}"
        if usage.get("territory"):
            util_fmt["territory"] = usage.get("territory")
        if usage.get("csg_geo"):
            util_fmt["csg_geo"] = usage.get("csg_geo")

        return {"ari_scores": ari_scores, "usage": util_fmt, "degraded": []}

    def analyze_risk(self, account_data: Dict[str, Any]) -> Dict[str, Any]:
        sf = account_data.get("salesforce", {})
        acc = sf.get("account") or {}
        red = sf.get("red_account")
        account_name = acc.get("Name") or str(account_data.get("account_id", "Account"))
        analytics = account_data.get("analytics", {})
        enr = self._enrichment_from_analytics(analytics)

        risk_reason = "N/A"
        risk_detail = ""
        if red:
            risk_reason = str(red.get("Issue_Product__c") or red.get("Stage__c") or "Red Account")
            risk_detail = str(red.get("Latest_Updates__c") or red.get("Action_Plan__c") or "")[:500]

        utilization = str((enr.get("usage") or {}).get("utilization_rate") or "")
        classification = classify_risk(
            risk_reason,
            risk_detail,
            str(acc.get("Industry") or acc.get("Type") or ""),
            utilization,
        )

        risk_notes, recommendation = generate_risk_analysis(
            account_name,
            None,
            red,
            enr,
            self.call_llm_fn,
        )

        top_cat = None
        for s in enr.get("ari_scores") or []:
            if s.get("category") and str(s.get("category")).upper() != "N/A":
                top_cat = s.get("category")
                break
        if top_cat is None and enr.get("ari_scores"):
            top_cat = (enr["ari_scores"][0] or {}).get("category")

        return {
            "summary": classification["theme"],
            "risk_notes": risk_notes,
            "recommendation": recommendation,
            "ari_category": top_cat,
            "ari_probability": analytics.get("ari_score"),
            "confidence": classification["confidence"],
            "license_at_risk_reason": risk_reason if risk_reason != "N/A" else None,
        }

    def generate_adoption_pov(self, account_data: Dict[str, Any]) -> Dict[str, Any]:
        analytics = account_data.get("analytics", {})
        usage = analytics.get("usage") or {}
        attrition = analytics.get("attrition") or {}
        products = attrition.get("products") or []

        def fmt_pct(v):
            if v is None:
                return None
            if isinstance(v, str):
                return v
            return f"{float(v):.1f}%"

        def fmt_money(v):
            if v is None:
                return None
            if isinstance(v, str):
                return v
            return f"${float(v):,.0f}"

        narrative_parts = []
        if products:
            narrative_parts.append(
                f"Snowflake attrition signals: **{len(products)}** product row(s) on latest snapshot."
            )

        return {
            "utilization_rate": fmt_pct(usage.get("utilization_rate")),
            "gmv_rate": fmt_pct(usage.get("gmv_rate")),
            "burn_rate": fmt_pct(usage.get("burn_rate")),
            "cc_aov": fmt_money(usage.get("cc_aov")),
            "territory": usage.get("territory"),
            "csg_geo": usage.get("csg_geo"),
            "narrative": "\n".join(narrative_parts) if narrative_parts else None,
        }
