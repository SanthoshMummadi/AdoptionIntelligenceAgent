from mcp.server.fastmcp import FastMCP
import PyPDF2
import json
import os
import pickle
import threading
import time
import urllib3
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from log_utils import log_debug

# Load environment variables
load_dotenv()

# Import GM Review components
from services.gm_review_workflow import GMReviewWorkflow

mcp = FastMCP("Product Adoption MCP")

# Suppress SSL warnings for internal Salesforce endpoints
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Shared LLM Gateway HTTP session (connection pooling; retries via call_llm_gateway_with_retry)
_llm_session: requests.Session | None = None
_llm_session_lock = threading.Lock()


def _get_llm_session() -> requests.Session:
    global _llm_session
    if _llm_session is None:
        with _llm_session_lock:
            if _llm_session is None:
                session = requests.Session()
                adapter = HTTPAdapter(
                    pool_connections=4,
                    pool_maxsize=8,
                    max_retries=Retry(total=0),
                )
                session.mount("https://", adapter)
                session.mount("http://", adapter)
                _llm_session = session
    return _llm_session


# Persistent storage directory
STORAGE_DIR = os.path.join(os.path.dirname(__file__), "storage")
os.makedirs(STORAGE_DIR, exist_ok=True)

BRIEFS_FILE = os.path.join(STORAGE_DIR, "user_briefs.pkl")


# ============================================================================
# PERSISTENT STORAGE HELPERS
# ============================================================================

def load_data():
    """Load user briefs from disk"""
    global user_briefs
    if os.path.exists(BRIEFS_FILE):
        try:
            with open(BRIEFS_FILE, "rb") as f:
                user_briefs = pickle.load(f)
            print(f"✓ Loaded {len(user_briefs)} users' briefs from disk")
        except Exception as e:
            print(f"⚠️  Could not load briefs: {e}")
            user_briefs = {}
    else:
        user_briefs = {}


def save_data():
    """Save user briefs to disk"""
    try:
        with open(BRIEFS_FILE, "wb") as f:
            pickle.dump(user_briefs, f)
    except Exception as e:
        print(f"❌ Error saving briefs: {e}")


user_briefs: dict[str, dict[str, str]] = {}
load_data()


def get_user_briefs(user_id: str):
    """Get or create briefs dictionary for a specific user"""
    if user_id not in user_briefs:
        user_briefs[user_id] = {}
    return user_briefs[user_id]


# ============================================================================
# LLM GATEWAY
# ============================================================================

def call_llm_gateway(prompt: str, system_prompt: str = None, max_tokens: int = 4000):
    """Call Salesforce LLM Gateway Express"""

    gateway_url = "https://eng-ai-model-gateway.sfproxy.devx-preprod.aws-esvc1-useast2.aws.sfdc.cl/v1/chat/completions"

    api_key = os.environ.get("LLM_GATEWAY_API_KEY")
    if not api_key:
        raise Exception("❌ LLM_GATEWAY_API_KEY not set")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": "claude-sonnet-4-5-20250929",
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": 0.7,
    }

    try:
        print(f"🔄 Calling LLM Gateway... (model: {payload['model']})")
        session = _get_llm_session()
        response = session.post(
            gateway_url,
            headers=headers,
            json=payload,
            timeout=180,
            verify=False,
        )
        response.raise_for_status()

        data = response.json()
        content = data["choices"][0]["message"]["content"]
        print(f"✓ LLM Gateway response received ({len(content)} chars)")
        return content

    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, "response") and e.response is not None:
            try:
                error_detail = e.response.json()
                error_msg = f"{error_msg}\nDetails: {json.dumps(error_detail, indent=2)}"
            except Exception:
                error_msg = f"{error_msg}\nResponse: {e.response.text}"
        print(f"❌ LLM Gateway error: {error_msg}")
        raise Exception(f"LLM Gateway error: {error_msg}")


def call_llm_gateway_with_retry(
    prompt: str,
    system_prompt: str = None,
    max_tokens: int = 4000,
    max_retries: int = 2,
    backoff: float = 2.0,
):
    """Call LLM Gateway with retry on timeout / rate limit / overload."""
    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            return call_llm_gateway(
                prompt, system_prompt=system_prompt, max_tokens=max_tokens
            )
        except Exception as e:
            last_error = e
            err_str = str(e).lower()
            if any(
                kw in err_str
                for kw in ("timeout", "rate", "429", "503", "overload")
            ):
                if attempt < max_retries:
                    wait = backoff * (2**attempt)
                    log_debug(
                        f"LLM Gateway error (attempt {attempt + 1}), "
                        f"retrying in {wait:.0f}s: {str(e)[:60]}"
                    )
                    time.sleep(wait)
                    continue
            raise
    assert last_error is not None
    raise last_error


# ============================================================================
# GM REVIEW WORKFLOW INITIALIZATION
# ============================================================================

def init_gm_workflow() -> GMReviewWorkflow:
    """Initialize GM Review workflow (domain calls + LLM gateway)."""
    return GMReviewWorkflow(
        call_llm_fn=call_llm_gateway_with_retry,
        max_concurrent=8,
    )


# ============================================================================
# PRODUCT BRIEF TOOLS
# ============================================================================

@mcp.tool()
def ping() -> str:
    """Check if MCP server is alive"""
    return "✓ Product Adoption MCP with persistent storage is running!"


@mcp.tool()
def upload_brief_text(brief_name: str, content: str, user_id: str = "default") -> str:
    """Upload a product brief document (text content)"""
    briefs = get_user_briefs(user_id)
    briefs[brief_name] = content
    save_data()
    return f"✓ Product brief '{brief_name}' uploaded successfully! ({len(content):,} characters)"


@mcp.tool()
def upload_brief_pdf(brief_name: str, file_path: str, user_id: str = "default") -> str:
    """Upload a product brief from a local PDF file"""
    try:
        with open(file_path, "rb") as pdf_file:
            reader = PyPDF2.PdfReader(pdf_file)
            text = ""
            for page in reader.pages:
                text += (page.extract_text() or "") + "\n"

        briefs = get_user_briefs(user_id)
        briefs[brief_name] = text.strip()
        save_data()

        return (
            f"✓ PDF brief '{brief_name}' uploaded! "
            f"({len(text):,} characters extracted from {len(reader.pages)} pages)"
        )

    except FileNotFoundError:
        return f"❌ Error: File not found at {file_path}"
    except Exception as e:
        return f"❌ Error reading PDF: {str(e)}"


@mcp.tool()
def query_brief(brief_name: str, prompt: str, user_id: str = "default") -> str:
    """Ask questions about a product brief using AI"""
    briefs = get_user_briefs(user_id)

    actual_name = None
    for name in briefs.keys():
        if name.lower() == brief_name.lower():
            actual_name = name
            break

    if not actual_name:
        available = ", ".join(briefs.keys()) if briefs else "None"
        return f"❌ Brief '{brief_name}' not found. Available briefs: {available}"

    content = briefs[actual_name]

    max_content_length = 100000
    if len(content) > max_content_length:
        content = content[:max_content_length] + "\n\n[Content truncated due to length...]"

    try:
        llm_prompt = f"""Here is a product brief document:

<product_brief>
{content}
</product_brief>

Please answer this question about the brief:
{prompt}

Provide a comprehensive, well-structured answer. Use clear formatting with:
- Numbered lists for sequential items
- Bullet points for related items
- Bold headers for sections
- Specific examples and quotes from the brief when relevant

Be thorough and detailed - don't truncate your response."""

        system_prompt = """You are a helpful AI assistant analyzing product briefs for Salesforce teams.

When summarizing adoption status, risks, or "what to focus on" for a product, use this style when appropriate:
- Open with a brief greeting (e.g. "Hey [name] 👋" if the context suggests it, or just "Here's where things stand").
- State what you're tracking (e.g. "I'm tracking [Product X] for you").
- Use a "Right now:" section with bullets: adoption trends, attrition/renewal signals, key dates.
- End with "What do you want to explore?" and offer options such as: Adoption risks, Renewal forecast, Feature usage gaps, V2MoM progress, Top accounts needing attention.
Keep the tone concise and actionable."""

        answer = call_llm_gateway_with_retry(llm_prompt, system_prompt, max_tokens=4000)
        return answer

    except Exception as e:
        return f"❌ Error calling LLM: {str(e)}"


@mcp.tool()
def list_briefs(user_id: str = "default") -> str:
    """List all uploaded product briefs for this user"""
    briefs = get_user_briefs(user_id)

    if not briefs:
        return "📪 No briefs uploaded yet.\n\nUpload a PDF to get started!"

    brief_list = "\n".join(
        [f"- *{name}* ({len(content):,} chars)" for name, content in briefs.items()]
    )
    return f"*Your uploaded briefs:*\n{brief_list}\n\nJust ask me anything about them!"


@mcp.tool()
def delete_brief(brief_name: str, user_id: str = "default") -> str:
    """Delete a product brief (case-insensitive)."""
    briefs = get_user_briefs(user_id)

    actual_name = None
    for name in briefs.keys():
        if name.lower() == brief_name.lower():
            actual_name = name
            break

    if not actual_name:
        return f"❌ Brief '{brief_name}' not found"

    del briefs[actual_name]
    save_data()
    return f"✓ Brief '{actual_name}' deleted successfully"


# ============================================================================
# GM REVIEW TOOLS
# ============================================================================

@mcp.tool()
def generate_gm_reviews(account_inputs: list[str]) -> str:
    """
    Generate GM reviews for at-risk Commerce Cloud renewals.

    Args:
        account_inputs: List of account names or opportunity IDs (e.g., ["Acme Corp", "006XXXXXXXXXXXX"])

    Returns:
        Formatted GM review results with risk analysis and adoption POV
    """
    try:
        print("🚀 Initializing GM Review workflow...")
        workflow = init_gm_workflow()

        print(f"📊 Processing {len(account_inputs)} accounts...")
        out = workflow.run(account_inputs)
        reviews = out.get("reviews") or []

        if not reviews:
            return "❌ No reviews generated. Check if account names/IDs are valid."

        result = f"✓ Generated {len(reviews)} GM review(s):\n\n"

        for review in reviews:
            result += f"**{review['account_name']}** (ID: {review['account_id']})\n"
            result += f"📋 Risk notes:\n{review.get('risk_notes', '')}\n\n"
            result += f"📌 Recommendations:\n{review.get('recommendation', '')}\n\n"
            result += f"📈 Adoption POV:\n{review.get('adoption_pov', '')}\n\n"
            result += "---\n\n"

        return result

    except Exception as e:
        return f"❌ Error generating reviews: {str(e)}"


@mcp.tool()
def generate_gm_review_canvas(account_inputs: list[str]) -> str:
    """
    Generate GM reviews and return canvas-formatted content for at-risk Commerce renewals.

    Args:
        account_inputs: List of account names or opportunity IDs

    Returns:
        Canvas-formatted markdown content
    """
    try:
        print("🚀 Initializing GM Review workflow...")
        workflow = init_gm_workflow()

        print(f"📊 Processing {len(account_inputs)} accounts...")
        out = workflow.run(account_inputs)
        combined = (out.get("combined_canvas") or "").strip()
        if not combined:
            return "❌ No reviews generated."
        return combined

    except Exception as e:
        return f"❌ Error generating canvas: {str(e)}"


@mcp.tool()
def test_snowflake_connection() -> str:
    """Test Snowflake connection and credentials"""
    try:
        from domain.analytics.snowflake_client import run_query

        run_query("SELECT 1 AS ok")
        return "✓ Snowflake connection successful!"
    except Exception as e:
        return f"❌ Snowflake connection failed: {str(e)}"


@mcp.tool()
def test_salesforce_connection() -> str:
    """Test Salesforce connection and credentials"""
    try:
        from domain.salesforce.org62_client import get_sf_client

        sf = get_sf_client()
        sf.query("SELECT Id FROM User LIMIT 1")
        return "✓ Salesforce session successful!"
    except Exception as e:
        return f"❌ Salesforce connection failed: {str(e)}"


if __name__ == "__main__":
    mcp.run()
