from mcp.server.fastmcp import FastMCP
import PyPDF2
import os
import requests
import json
import pickle
import urllib3

mcp = FastMCP("Product Brief MCP")

# Suppress SSL warnings for internal Salesforce endpoints
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Persistent storage directory
STORAGE_DIR = os.path.join(os.path.dirname(__file__), "storage")
os.makedirs(STORAGE_DIR, exist_ok=True)

BRIEFS_FILE = os.path.join(STORAGE_DIR, "user_briefs.pkl")


def load_data():
    """Load user briefs from disk"""
    global user_briefs
    if os.path.exists(BRIEFS_FILE):
        try:
            with open(BRIEFS_FILE, "rb") as f:
                user_briefs = pickle.load(f)
            print(f"✅ Loaded {len(user_briefs)} users' briefs from disk")
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
        response = requests.post(
            gateway_url,
            headers=headers,
            json=payload,
            timeout=180,
            verify=False,
        )
        response.raise_for_status()

        data = response.json()
        content = data["choices"][0]["message"]["content"]
        print(f"✅ LLM Gateway response received ({len(content)} chars)")
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


@mcp.tool()
def ping() -> str:
    """Check if MCP server is alive"""
    return "✅ Product Brief MCP with persistent storage is running!"


@mcp.tool()
def upload_brief_text(brief_name: str, content: str, user_id: str = "default") -> str:
    """Upload a product brief document (text content)"""
    briefs = get_user_briefs(user_id)
    briefs[brief_name] = content
    save_data()
    return f"✅ Product brief '{brief_name}' uploaded successfully! ({len(content):,} characters)"


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

        return f"✅ PDF brief '{brief_name}' uploaded! ({len(text):,} characters extracted from {len(reader.pages)} pages)"

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
- Open with a brief greeting (e.g. "Hey [name] :wave:" if the context suggests it, or just "Here's where things stand").
- State what you're tracking (e.g. "I'm tracking [Product X] for you").
- Use a "Right now:" section with bullets: adoption trends, attrition/renewal signals, key dates.
- End with "What do you want to explore?" and offer options such as: Adoption risks, Renewal forecast, Feature usage gaps, V2MoM progress, Top accounts needing attention.
Keep the tone concise and actionable."""

        answer = call_llm_gateway(llm_prompt, system_prompt, max_tokens=4000)
        return answer

    except Exception as e:
        return f"❌ Error calling LLM: {str(e)}"


@mcp.tool()
def list_briefs(user_id: str = "default") -> str:
    """List all uploaded product briefs for this user"""
    briefs = get_user_briefs(user_id)

    if not briefs:
        return "📭 No briefs uploaded yet.\n\nUpload a PDF to get started!"

    brief_list = "\n".join(
        [f"• *{name}* ({len(content):,} chars)" for name, content in briefs.items()]
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
    return f"✅ Brief '{actual_name}' deleted successfully"


if __name__ == "__main__":
    mcp.run()

