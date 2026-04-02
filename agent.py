"""
agent.py
Intent classification and routing using Claude.
"""
import json
from server import call_llm_gateway
from log_utils import log_debug


def classify_with_claude(text: str, last_account: str, last_cloud: str, conversation_history: list) -> dict:
    """
    Classify user intent and extract parameters using Claude.
    Returns: {"tool": "<tool_name>", "params": {<extracted parameters>}}
    """

    system_prompt = """You are a routing agent for a Salesforce PM Intelligence bot.
Analyze the user's message and return a JSON object with:
{
  "tool": "<tool_name>",
  "params": {<extracted parameters>}
}

Available tools:
- get_account_brief: Single account lookup (default for account names)
- get_at_risk_list: User wants a filtered list of at-risk accounts
- get_account_field: User asks for specific field(s) for account(s)
- get_bulk_ari: User asks for ARI scores for multiple accounts
- create_gm_review: User explicitly wants GM Review canvas
- compare_accounts: User wants to compare two accounts
- reset_session: User wants to clear context
- answer_general: General question not about specific accounts

Parameter extraction:
- account_name: Single account name (str)
- accounts: Multiple account names (list)
- fields: Specific fields requested like ["ari", "health", "atr"] (list)
- cloud: Detected cloud (str) - default "Commerce Cloud"
- account_a, account_b: For comparisons

Examples:
"What's the ARI for Titan?" → {"tool": "get_account_field", "params": {"accounts": ["Titan"], "fields": ["ari"]}}
"Show me high risk accounts" → {"tool": "get_at_risk_list", "params": {}}
"Acme Corp" → {"tool": "get_account_brief", "params": {"account_name": "Acme Corp"}}
"What's the health and utilization for Titan and Acme?" → {"tool": "get_account_field", "params": {"accounts": ["Titan", "Acme"], "fields": ["health", "utilization"]}}
"Compare Titan vs Acme" → {"tool": "compare_accounts", "params": {"account_a": "Titan", "account_b": "Acme"}}
"reset" or "clear" → {"tool": "reset_session", "params": {}}
"""

    context = f"Last account discussed: {last_account or 'None'}\n"
    context += f"Last cloud: {last_cloud}\n"
    if conversation_history:
        recent = conversation_history[-2:] if len(conversation_history) >= 2 else conversation_history
        context += f"Recent messages: {recent}\n"

    prompt = f"""{context}
User message: "{text}"

Classify this and return JSON:"""

    try:
        response = call_llm_gateway(prompt, system_prompt=system_prompt, max_tokens=200)

        # Extract JSON from response
        response = response.strip()
        if response.startswith("```json"):
            response = response.split("```json")[1].split("```")[0].strip()
        elif response.startswith("```"):
            response = response.split("```")[1].split("```")[0].strip()

        result = json.loads(response)
        log_debug(f"Agent classified: {result.get('tool')} with params: {result.get('params')}")
        return result

    except Exception as e:
        log_debug(f"Agent classification error: {e}")
        # Default fallback: treat as account lookup
        return {
            "tool": "get_account_brief",
            "params": {"account_name": text, "cloud": last_cloud},
        }
