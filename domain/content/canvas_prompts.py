"""
canvas_prompts.py
Canvas prompt management for hub modules.
"""


def fetch_hub_items(client):
    """
    Fetch hub items from prompt canvas.
    Returns list of hub sections.
    """
    # Stub for now - can be expanded to read from a central prompt canvas
    return [
        {"section": "product_brief", "label": "Product Brief Analysis"},
        {"section": "v2mom", "label": "V2MoM Analysis"},
        {"section": "attrition_risk", "label": "Attrition Risk Predictor"},
        {"section": "feature_scorecard", "label": "Feature Usage Scorecard"},
    ]


def fetch_prompts_from_canvas(canvas_id: str, client):
    """
    Fetch prompts from a specific canvas.
    Returns list of prompt objects.
    """
    # Stub - would use canvases.sections.list API
    return []


def fetch_section_prompts(section: str, client):
    """
    Fetch prompts for a specific section.
    Returns list of {label, prompt} dicts.
    """
    # Hardcoded prompts for each section
    prompts = {
        "product_brief": [
            {
                "label": "Executive Summary",
                "prompt": "Provide a 3-paragraph executive summary of this product brief suitable for VP-level audience. Include: strategic vision, key initiatives, and business impact.",
            },
            {
                "label": "Key Features & Benefits",
                "prompt": "List the top 5 features mentioned in this brief with their business benefits. Format as bullet points with feature name and 1-sentence benefit.",
            },
            {
                "label": "Risk Analysis",
                "prompt": "Identify the top 3 risks mentioned in this brief. For each risk, provide: risk description, potential impact, and suggested mitigation.",
            },
            {
                "label": "Competitive Positioning",
                "prompt": "Summarize how this product positions against competitors. Include key differentiators and competitive advantages.",
            },
            {
                "label": "Adoption Metrics",
                "prompt": "Extract and summarize all adoption metrics, KPIs, and success criteria mentioned in the brief.",
            },
        ],
        "v2mom": [
            {
                "label": "Vision Alignment",
                "prompt": "How does this brief align with the V2MoM vision? Identify gaps and opportunities.",
            },
            {
                "label": "Method Progress",
                "prompt": "Assess progress against each Method in the V2MoM. Which methods are on track? Which need attention?",
            },
            {
                "label": "Obstacle Analysis",
                "prompt": "Identify obstacles from the V2MoM that this brief addresses. Are there new obstacles not covered?",
            },
            {
                "label": "Measure Tracking",
                "prompt": "Review all Measures in the V2MoM. Which measures are being tracked in this brief? Which are missing?",
            },
        ],
        "attrition_risk": [
            {
                "label": "Risk Summary",
                "prompt": "Summarize attrition risk for this account including ARI score, utilization rate, and key risk factors.",
            },
        ],
        "feature_scorecard": [
            {
                "label": "Feature Usage Report",
                "prompt": "Generate a feature usage scorecard showing adoption rates and engagement levels.",
            },
        ],
    }

    return prompts.get(section, [])


def send_hub_menu(say_fn):
    """Send hub menu to user (called from handle_message)."""
    say_fn(
        "*PM Intelligence Hub*\n\n"
        "Choose a module:\n"
        "- 📋 Product Brief Analysis - `/analyze-brief`\n"
        "- 🎯 V2MoM Analysis - Type 'v2mom'\n"
        "- ⚠️ Attrition Risk - `/attrition-risk <Account Name>`\n"
        "- 📊 Feature Scorecard - Coming soon\n\n"
        "_Or just upload a PDF and start asking questions!_"
    )
