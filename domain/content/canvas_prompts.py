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
