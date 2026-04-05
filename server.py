from mcp.server.fastmcp import FastMCP
import PyPDF2
import json
import os
import pickle
import sys
import threading
import time
import urllib3
import certifi
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from log_utils import log_debug, log_error, log_structured

# Load environment variables
load_dotenv()


def _validate_required_env() -> None:
    """Validate required env vars at startup — fail fast with clear error."""
    required = {
        "SNOWFLAKE_USER": "Snowflake username",
        "SNOWFLAKE_ACCOUNT": "Snowflake account identifier",
        "LLM_GATEWAY_API_KEY": "LLM Gateway API key",
    }

    # Salesforce: either session token + instance, or username + password
    sf_valid = (
        (os.getenv("SALESFORCE_SESSION_ID") and os.getenv("SALESFORCE_INSTANCE_URL"))
        or (os.getenv("SALESFORCE_USERNAME") and os.getenv("SALESFORCE_PASSWORD"))
    )

    missing: list[str] = []
    for key, description in required.items():
        if not os.getenv(key):
            missing.append(f"  - {key} ({description})")

    if not sf_valid:
        missing.append(
            "  - Salesforce auth: need either (SALESFORCE_SESSION_ID + SALESFORCE_INSTANCE_URL) "
            "or (SALESFORCE_USERNAME + SALESFORCE_PASSWORD)"
        )

    if missing:
        error_msg = "❌ Missing required environment variables:\n" + "\n".join(missing)
        error_msg += "\n\nCheck your .env file and ensure all required vars are set."
        log_error(error_msg)
        raise RuntimeError(error_msg)

    if os.getenv("LLM_GATEWAY_VERIFY", "").strip().lower() == "false":
        log_error(
            "⚠️ WARNING: LLM_GATEWAY_VERIFY=false detected — ignored; "
            "LLM gateway always verifies TLS (certifi, LLM_GATEWAY_CA_BUNDLE, or system CAs)."
        )

    log_debug("✓ All required environment variables present")


def _should_run_startup_env_validation() -> bool:
    """Skip when tests import this module without full .env (pytest or scripts under tests/)."""
    if os.getenv("PRODUCT_ADOPTION_SKIP_ENV_VALIDATION", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        return False
    if "pytest" in sys.modules:
        return False
    main = sys.modules.get("__main__")
    main_file = getattr(main, "__file__", None) if main else None
    if main_file:
        parts = os.path.normpath(os.path.abspath(main_file)).split(os.sep)
        if "tests" in parts:
            return False
    return True


if _should_run_startup_env_validation():
    _validate_required_env()

# Import GM Review components
from services.gm_review_workflow import GMReviewWorkflow

mcp = FastMCP("Product Adoption MCP")

# Suppress SSL warnings for internal Salesforce endpoints
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Shared LLM Gateway HTTP session (connection pooling; retries via call_llm_gateway_with_retry)
_llm_session: requests.Session | None = None
_llm_session_lock = threading.Lock()

_llm_failure_count = 0
_llm_circuit_open_until = 0.0
_llm_cb_lock = threading.Lock()


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except (TypeError, ValueError):
        return default


LLM_CIRCUIT_THRESHOLD = _env_int("LLM_CIRCUIT_THRESHOLD", 3)
LLM_CIRCUIT_COOLDOWN = _env_int("LLM_CIRCUIT_COOLDOWN", 300)


def _llm_circuit_is_open() -> bool:
    global _llm_circuit_open_until, _llm_failure_count
    if _llm_circuit_open_until == 0.0:
        return False
    if time.time() > _llm_circuit_open_until:
        with _llm_cb_lock:
            _llm_circuit_open_until = 0.0
            _llm_failure_count = 0
        log_debug("LLM circuit breaker RESET — resuming normal mode")
        log_structured("llm_circuit_reset")
        return False
    return True


def _llm_record_failure() -> None:
    global _llm_failure_count, _llm_circuit_open_until
    with _llm_cb_lock:
        _llm_failure_count += 1
        if _llm_failure_count >= LLM_CIRCUIT_THRESHOLD:
            _llm_circuit_open_until = time.time() + LLM_CIRCUIT_COOLDOWN
            log_error(
                f"LLM circuit breaker OPEN — {_llm_failure_count} consecutive failures. "
                f"Degraded mode for {LLM_CIRCUIT_COOLDOWN}s."
            )
            log_structured(
                "llm_circuit_open",
                level="error",
                failure_count=_llm_failure_count,
                cooldown_s=LLM_CIRCUIT_COOLDOWN,
            )


def _llm_record_success() -> None:
    global _llm_failure_count
    with _llm_cb_lock:
        _llm_failure_count = 0


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

_llm_verify_resolved: str | bool | None = None


def _get_llm_verify_config() -> str | bool:
    """
    TLS verification for LLM gateway requests.

    Priority: custom CA bundle (LLM_GATEWAY_CA_BUNDLE) > certifi > system default.
    verify=False is not used; LLM_GATEWAY_VERIFY=false is warned at startup only.
    """
    global _llm_verify_resolved
    if _llm_verify_resolved is not None:
        return _llm_verify_resolved

    custom_ca = os.getenv("LLM_GATEWAY_CA_BUNDLE")
    if custom_ca:
        if os.path.isfile(custom_ca):
            log_debug(f"Using custom CA bundle for LLM gateway: {custom_ca}")
            _llm_verify_resolved = custom_ca
            return _llm_verify_resolved
        log_error(f"Custom CA bundle not found: {custom_ca}")

    try:
        ca_path = certifi.where()
        log_debug(f"Using certifi CA bundle for LLM gateway: {ca_path}")
        _llm_verify_resolved = ca_path
        return _llm_verify_resolved
    except Exception:
        pass

    log_debug("Using system default CA bundle for LLM gateway")
    _llm_verify_resolved = True
    return _llm_verify_resolved


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

    t_llm = time.time()
    model = payload["model"]
    try:
        print(f"🔄 Calling LLM Gateway... (model: {model})")
        session = _get_llm_session()
        response = session.post(
            gateway_url,
            headers=headers,
            json=payload,
            timeout=180,
            verify=_get_llm_verify_config(),
        )
        response.raise_for_status()

        data = response.json()
        content = data["choices"][0]["message"]["content"]
        print(f"✓ LLM Gateway response received ({len(content)} chars)")
        log_structured(
            "llm_call",
            status="ok",
            latency_ms=round((time.time() - t_llm) * 1000),
            model=model,
        )
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

    except Exception as e:
        log_structured(
            "llm_call",
            level="error",
            status="error",
            error=str(e)[:80],
            model=model,
        )
        raise


def call_llm_gateway_with_retry(
    prompt: str,
    system_prompt: str = None,
    max_tokens: int = 4000,
    max_retries: int = 2,
    backoff: float = 2.0,
) -> str:
    """
    LLM call with circuit breaker + retry. Returns '' in degraded mode (circuit open).

    Transient errors (timeout, 429, 503, overload, rate) increment the failure counter
    and retry; non-transient errors re-raise without opening the circuit.
    """
    if _llm_circuit_is_open():
        log_debug("LLM circuit breaker OPEN — skipping LLM (degraded mode)")
        return ""

    last_error: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            result = call_llm_gateway(
                prompt, system_prompt=system_prompt, max_tokens=max_tokens
            )
            _llm_record_success()
            return result
        except Exception as e:
            last_error = e
            log_structured(
                "llm_call",
                level="error",
                status="error",
                error=str(e)[:80],
                attempt=attempt + 1,
            )
            err_str = str(e).lower()
            is_transient = any(
                kw in err_str
                for kw in ("timeout", "429", "503", "overload", "rate")
            )
            if is_transient:
                _llm_record_failure()
                if attempt < max_retries:
                    wait = backoff * (2**attempt)
                    log_debug(
                        f"LLM transient error (attempt {attempt + 1}), "
                        f"retrying in {wait:.0f}s: {str(e)[:60]}"
                    )
                    time.sleep(wait)
                    continue
                raise last_error
            raise


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
    """Minimal liveness check — no I/O."""
    return "pong"


def _health_salesforce_env_ok() -> tuple[bool, list[str]]:
    """True if token+instance or username+password are set (matches org62_client)."""
    token = os.getenv("SF_ACCESS_TOKEN") or os.getenv("SALESFORCE_ACCESS_TOKEN")
    inst = os.getenv("SF_INSTANCE_URL") or os.getenv("SALESFORCE_INSTANCE_URL")
    user = os.getenv("SF_USERNAME")
    password = os.getenv("SF_PASSWORD")
    if token and inst:
        return True, []
    if user and password:
        return True, []
    hints: list[str] = []
    if token and not inst:
        hints.append("SF_INSTANCE_URL or SALESFORCE_INSTANCE_URL")
    elif user and not password:
        hints.append("SF_PASSWORD (with SF_USERNAME)")
    elif not token and not user:
        hints.append(
            "SF_ACCESS_TOKEN+SF_INSTANCE_URL (or SALESFORCE_*), or SF_USERNAME+SF_PASSWORD"
        )
    else:
        hints.append("Salesforce credentials incomplete (see org62_client)")
    return False, hints


@mcp.tool()
def health_check() -> dict:
    """
    Lightweight readiness: env vars, Snowflake pool state, Salesforce ping, LLM circuit.
    No heavy Snowflake queries — safe to call frequently.
    """
    results: dict = {}
    overall = "ok"

    # 1. Env vars
    required_snowflake = ["SNOWFLAKE_USER", "SNOWFLAKE_ACCOUNT"]
    missing = [k for k in required_snowflake if not os.getenv(k)]
    if not os.getenv("LLM_GATEWAY_API_KEY"):
        missing.append("LLM_GATEWAY_API_KEY")
    sf_ok, sf_missing = _health_salesforce_env_ok()
    if not sf_ok:
        missing.extend(sf_missing)
    if missing:
        results["env"] = {"status": "error", "missing": list(dict.fromkeys(missing))}
        overall = "degraded"
    else:
        results["env"] = {"status": "ok"}

    # 2. Snowflake pool (no query — pool may be lazy until first use)
    try:
        from domain.analytics.snowflake_client import POOL_SIZE, _pool, _pool_initialized

        if not _pool_initialized:
            results["snowflake"] = {
                "status": "not_initialized",
                "pool_total": POOL_SIZE,
            }
        else:
            results["snowflake"] = {
                "status": "ok",
                "pool_available": _pool.qsize(),
                "pool_total": POOL_SIZE,
            }
    except Exception as e:
        results["snowflake"] = {"status": "error", "error": str(e)[:80]}
        overall = "degraded"

    # 3. Salesforce — lightweight ping (sf_query is semaphore-guarded)
    try:
        from domain.salesforce.org62_client import sf_query

        t0 = time.time()
        sf_query("SELECT Id FROM User LIMIT 1")
        results["salesforce"] = {
            "status": "ok",
            "latency_ms": round((time.time() - t0) * 1000),
        }
    except Exception as e:
        results["salesforce"] = {"status": "error", "error": str(e)[:80]}
        overall = "degraded"

    # 4. LLM circuit breaker (process-local)
    with _llm_cb_lock:
        fc = _llm_failure_count
    results["llm_circuit"] = {
        "status": "open" if _llm_circuit_is_open() else "closed",
        "failure_count": fc,
    }

    results["overall"] = overall
    return results


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
        if answer and str(answer).strip():
            return answer
        return (
            "⚠️ AI is temporarily unavailable (LLM circuit open or empty response). "
            "Try again in a few minutes, or use the non-LLM views in the app."
        )

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
        from domain.salesforce.org62_client import sf_query

        sf_query("SELECT Id FROM User LIMIT 1")
        return "✓ Salesforce session successful!"
    except Exception as e:
        return f"❌ Salesforce connection failed: {str(e)}"


if __name__ == "__main__":
    mcp.run()
