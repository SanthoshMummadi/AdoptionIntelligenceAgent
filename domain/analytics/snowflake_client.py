"""
domain/analytics/snowflake_client.py
Snowflake enrichment — CSS attrition uses MAX(SNAPSHOT_DT) (no CURR_SNAP) + renewal view + shims.
"""
import os
import re
import threading
import time
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Empty, Queue
from typing import Any, Optional

import snowflake.connector
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
from log_utils import log_debug, log_error, log_structured

load_dotenv()

_RENEWAL_VIEWS_PRIORITY = [
    "SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_VW",
    "SSE_DM_CSG_RPT_PRD.RENEWALS.CI_NEAR_REALTIME_RENEWAL_OPTY_VW",
    "SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_SNAP_VW",
]


def _get_renewal_view() -> str:
    """Returns first accessible renewal view from priority list."""
    return _RENEWAL_VIEWS_PRIORITY[0]

def _fmt_exc(e: BaseException) -> str:
    msg = str(e) if e is not None else ""
    if msg:
        return f"{type(e).__name__}: {msg}"
    return f"{type(e).__name__}: {repr(e)}"


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


def _env_pool_size() -> int:
    try:
        return max(1, int(os.getenv("SNOWFLAKE_POOL_SIZE", "8")))
    except (TypeError, ValueError):
        return 8


POOL_SIZE = _env_pool_size()
INITIAL_POOL_SIZE = max(
    1,
    min(
        POOL_SIZE,
        _env_int(
            "SNOWFLAKE_POOL_INITIAL_SIZE",
            min(4, POOL_SIZE),
        ),
    ),
)

# Cache for latest CIDM usage snapshot date (within a workflow run)
_usage_snapshot_cache: dict[str, Any] = {}
_usage_snapshot_lock = threading.Lock()

# Process-level TTL cache for account resolution + enrichment
_ACCOUNT_CACHE_TTL_S = _env_int("SNOWFLAKE_ACCOUNT_CACHE_TTL_S", 900)  # 15 min default
_account_resolve_cache: dict[str, tuple[Any, float]] = {}
_account_enrich_cache: dict[str, tuple[Any, float]] = {}
_account_cache_lock = threading.Lock()

_PREWARM_REFRESH_INTERVAL_S = _env_int("SNOWFLAKE_PREWARM_REFRESH_S", 86400)


def _cache_get(cache: dict, key: str) -> Any:
    with _account_cache_lock:
        entry = cache.get(key)
        if entry and (time.time() - entry[1]) < _ACCOUNT_CACHE_TTL_S:
            return entry[0]
        cache.pop(key, None)
    return None


def _cache_set(cache: dict, key: str, value: Any) -> None:
    with _account_cache_lock:
        cache[key] = (value, time.time())


def _should_refresh_prewarm(cache_key: str) -> bool:
    """True if prewarm cache is missing or older than ``SNOWFLAKE_PREWARM_REFRESH_S``."""
    entry = _usage_snapshot_cache.get(f"{cache_key}_cached_at")
    if not entry:
        return True
    try:
        return (time.time() - float(entry)) > _PREWARM_REFRESH_INTERVAL_S
    except (TypeError, ValueError):
        return True


def clear_stale_caches() -> None:
    """Remove TTL-expired entries from process-level account caches."""
    now = time.time()
    removed = 0
    with _account_cache_lock:
        stale_r = [
            k for k, (_, ts) in _account_resolve_cache.items()
            if (now - ts) > _ACCOUNT_CACHE_TTL_S
        ]
        for k in stale_r:
            del _account_resolve_cache[k]
        removed += len(stale_r)
        stale_e = [
            k for k, (_, ts) in _account_enrich_cache.items()
            if (now - ts) > _ACCOUNT_CACHE_TTL_S
        ]
        for k in stale_e:
            del _account_enrich_cache[k]
        removed += len(stale_e)
    log_debug(f"clear_stale_caches: removed {removed} entries")


def clear_usage_snapshot_cache() -> None:
    """Clear CIDM snapshot cache and failure backoff (call once per GM Review batch start)."""
    _usage_snapshot_cache.clear()


def prewarm_cidm_usage_snapshot_dt() -> None:
    """
    Run the global CIDM MAX(SNAPSHOT_DT) once while serialised.
    Call after ``clear_usage_snapshot_cache()`` at batch start so parallel
    ``enrich_account`` calls hit cache instead of stampeding Snowflake.
    """
    cache_key = "cidm_wv_av_usage_extract_vw_max_snapshot_dt"
    cached_at_key = f"{cache_key}_cached_at"
    if not _should_refresh_prewarm(cache_key):
        log_debug("CIDM prewarm: cache still fresh, skipping")
        return
    prev_at = _usage_snapshot_cache.get(cached_at_key)
    ignore_env = (
        prev_at is not None
        and (time.time() - float(prev_at)) > _PREWARM_REFRESH_INTERVAL_S
    )
    try:
        with _usage_snapshot_lock:
            _usage_snapshot_cache.pop(cache_key, None)
        _get_latest_cidm_usage_snapshot_dt(ignore_env_override=ignore_env)
        with _usage_snapshot_lock:
            _usage_snapshot_cache[cached_at_key] = time.time()
    except Exception as e:
        log_debug(f"prewarm_cidm: {_fmt_exc(e)[:100]}")


def prewarm_renewal_as_of_date() -> None:
    """Cache MAX(AS_OF_DATE) for renewal view at batch start."""
    key = "renewal_as_of_date"
    cached_at_key = f"{key}_cached_at"
    if not _should_refresh_prewarm(key):
        log_debug("Renewal prewarm: cache still fresh, skipping")
        return
    prev_at = _usage_snapshot_cache.get(cached_at_key)
    ignore_env = (
        prev_at is not None
        and (time.time() - float(prev_at)) > _PREWARM_REFRESH_INTERVAL_S
    )
    override = (os.getenv("SNOWFLAKE_RENEWAL_AS_OF_DATE") or "").strip()
    if override and not ignore_env:
        _usage_snapshot_cache[key] = override
        with _usage_snapshot_lock:
            _usage_snapshot_cache[cached_at_key] = time.time()
        return
    try:
        with _usage_snapshot_lock:
            _usage_snapshot_cache.pop(key, None)
        rows = run_query(
            "SELECT MAX(AS_OF_DATE) AS MAX_DATE "
            "FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_SNAP_VW",
            [],
            statement_timeout=120,
        )
        max_date = (rows[0] or {}).get("MAX_DATE") if rows else None
        if max_date:
            _usage_snapshot_cache[key] = max_date
            log_debug(f"✓ Renewal AS_OF_DATE prewarmed: {max_date}")
        with _usage_snapshot_lock:
            _usage_snapshot_cache[cached_at_key] = time.time()
    except Exception as e:
        log_debug(f"prewarm_renewal_as_of_date: {str(e)[:80]}")


def _get_latest_cidm_usage_snapshot_dt(ignore_env_override: bool = False) -> Any:
    """
    Get MAX(SNAPSHOT_DT) for CIDM usage view.
    Cached; serialized under ``_usage_snapshot_lock``. On repeated 57014 / failures,
    applies a short backoff so parallel GM Review accounts do not each re-run the
    same heavy MAX. Optional ``SNOWFLAKE_CIDM_SNAPSHOT_DT=YYYY-MM-DD`` skips the query.
    """
    cache_key = "cidm_wv_av_usage_extract_vw_max_snapshot_dt"
    fail_key = f"{cache_key}_fail_until"
    override = (os.getenv("SNOWFLAKE_CIDM_SNAPSHOT_DT") or "").strip()

    with _usage_snapshot_lock:
        if cache_key in _usage_snapshot_cache:
            return _usage_snapshot_cache[cache_key]

        if override and not ignore_env_override:
            _usage_snapshot_cache[cache_key] = override
            log_debug(
                "CIDM snapshot from SNOWFLAKE_CIDM_SNAPSHOT_DT "
                f"(MAX query skipped): {override!r}"
            )
            return override

        now = time.time()
        fail_until = float(_usage_snapshot_cache.get(fail_key) or 0.0)
        if fail_until > now:
            log_debug(
                "CIDM MAX(SNAPSHOT_DT) in failure backoff "
                f"({fail_until - now:.0f}s left) — skipping usage snapshot lookup"
            )
            return None

        if fail_key in _usage_snapshot_cache:
            del _usage_snapshot_cache[fail_key]

        stmt_to = _env_int("SNOWFLAKE_USAGE_MAX_SNAPSHOT_TIMEOUT", 120)
        attempts = max(1, _env_int("SNOWFLAKE_USAGE_MAX_SNAPSHOT_ATTEMPTS", 3))
        sleep_s = _env_int("SNOWFLAKE_USAGE_MAX_SNAPSHOT_RETRY_SLEEP_S", 4)
        max_date = None
        last_exc: Optional[BaseException] = None

        for attempt in range(attempts):
            try:
                rows = run_query(
                    """
                    SELECT MAX(SNAPSHOT_DT) AS MAX_DATE
                    FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
                    """,
                    [],
                    statement_timeout=stmt_to,
                )
                max_date = (rows[0] or {}).get("MAX_DATE") if rows else None
                if max_date is not None:
                    break
            except Exception as e:
                last_exc = e
                log_debug(
                    f"_get_latest_cidm_usage_snapshot_dt attempt {attempt + 1}/"
                    f"{attempts}: {_fmt_exc(e)[:120]}"
                )
                if attempt + 1 < attempts:
                    time.sleep(sleep_s * (attempt + 1))

        if max_date is not None:
            _usage_snapshot_cache.pop(fail_key, None)
            _usage_snapshot_cache[cache_key] = max_date
            return max_date

        backoff = max(30, _env_int("SNOWFLAKE_CIDM_SNAPSHOT_BACKOFF_S", 120))
        _usage_snapshot_cache[fail_key] = now + backoff
        if last_exc:
            log_debug(
                f"_get_latest_cidm_usage_snapshot_dt giving up for {backoff}s: "
                f"{_fmt_exc(last_exc)[:100]}"
            )
        return None

# Corporate suffix patterns for account-name stripping (fuzzy resolution)
CORPORATE_SUFFIXES = (
    r"\b(Inc\.?|LLC\.?|Ltd\.?|Corp\.?|Holdings?\.?|Holding\.?|Group|GmbH|Co\.?|"
    r"AB|SE|PLC|SA|AG|NV|BV|Pty\.?|Pte\.?|S\.A\.?|S\.L\.?|KGaA?|Limited|Company|"
    r"Enterprises?)\b"
)

SUCCESS_PLAN_KEYWORDS = [
    "success plan",
    "success plans",
    "- premier",
    "- signature",
    "- standard",
]

# AOV column/header label — driven by user / workflow cloud (slash command), not TARGET_CLOUD
# (Snowflake often returns TARGET_CLOUD = "Core" for FSC/industry).
CLOUD_AOV_LABEL_MAP: dict[str, str] = {
    "Commerce Cloud": "Commerce AOV",
    "B2C Commerce": "Commerce AOV",
    "B2B Commerce": "Commerce AOV",
    "Financial Services Cloud": "FSC AOV",
    "FSC": "FSC AOV",
    "Marketing Cloud": "Marketing AOV",
    "Sales Cloud": "Sales AOV",
    "Service Cloud": "Service AOV",
    "Data Cloud": "Data AOV",
    "Tableau": "Tableau AOV",
    "MuleSoft": "MuleSoft AOV",
    "Health Cloud": "Health AOV",
    "Industries": "Industries AOV",
}


def cloud_aov_label(cloud: str | None) -> str:
    """
    Return cloud-specific AOV label for sheet/canvas.
    Driven by user-selected cloud from GM Review workflow (slash command),
    not by renewal TARGET_CLOUD (often ``Core`` for FSC).
    """
    if not cloud or str(cloud).strip() in ("", "All Clouds"):
        return "Cloud AOV"
    c = str(cloud).strip()
    if c in CLOUD_AOV_LABEL_MAP:
        return CLOUD_AOV_LABEL_MAP[c]
    c_lower = c.lower()
    if "commerce" in c_lower:
        return "Commerce AOV"
    if "financial" in c_lower:
        return "FSC AOV"
    if "fsc" in c_lower:
        return "FSC AOV"
    if "marketing" in c_lower:
        return "Marketing AOV"
    if "sales" in c_lower:
        return "Sales AOV"
    if "service" in c_lower:
        return "Service AOV"
    if "data" in c_lower:
        return "Data AOV"
    if "tableau" in c_lower:
        return "Tableau AOV"
    if "mulesoft" in c_lower:
        return "MuleSoft AOV"
    if "health" in c_lower:
        return "Health AOV"
    parts = c.split()
    first = parts[0] if parts else "Cloud"
    return f"{first} AOV"


def _renewal_cloud_filter_sql(cloud: str, alias: str = "") -> str:
    """Predicate for renewal-view cloud matching against semicolon-style TARGET_CLOUD values."""
    prefix = f"{alias}." if alias else ""
    cloud_raw = str(cloud or "").strip()
    cloud_low = cloud_raw.lower()

    if "financial services" in cloud_low or cloud_raw.upper() == "FSC":
        return (
            f"({prefix}RENEWAL_OPTY_NM LIKE '%%Financial Services%%' "
            f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%FSC%%' "
            f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%Wealth%%' "
            f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%Insurance%%' "
            f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%Banking%%' "
            f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%Lending%%')"
        )

    if "commerce" in cloud_low:
        return (
            f"({prefix}TARGET_CLOUD LIKE '%%Commerce Cloud%%' "
            f"OR {prefix}TARGET_CLOUD LIKE '%%B2C%%' "
            f"OR {prefix}TARGET_CLOUD LIKE '%%B2B Commerce%%' "
            f"OR {prefix}TARGET_CLOUD LIKE '%%Order Management%%' "
            f"OR {prefix}TARGET_CLOUD LIKE '%%OMS%%' "
            f"OR {prefix}TARGET_CLOUD LIKE '%%Digital%%' "
            f"OR {prefix}TARGET_CLOUD LIKE '%%Merch%%' "
            f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%Commerce%%' "
            f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%B2C%%' "
            f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%B2B%%' "
            f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%Order Management%%')"
        )

    cloud_safe = cloud_raw.replace("'", "''").replace("%", "%%")
    return (
        f"({prefix}TARGET_CLOUD LIKE '%%{cloud_safe}%%' "
        f"OR {prefix}RENEWAL_OPTY_NM LIKE '%%{cloud_safe}%%')"
    )


_pool: Queue = Queue(maxsize=POOL_SIZE)
_pool_initialized = False
_pool_created_count = 0
_pool_lock = threading.Lock()


def _snowflake_conn_params() -> dict[str, Any]:
    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE") or "DEMO_WH"
    database = os.getenv("SNOWFLAKE_DATABASE") or "SSE_DM_CSG_RPT_PRD"
    schema = os.getenv("SNOWFLAKE_SCHEMA") or "RENEWALS"
    role = os.getenv("SNOWFLAKE_ROLE") or None
    password = os.getenv("SNOWFLAKE_PASSWORD")
    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "")

    if not account or not user:
        raise Exception("Missing SNOWFLAKE_ACCOUNT or SNOWFLAKE_USER in .env")

    params: dict[str, Any] = {
        "user": user,
        "account": account,
        "warehouse": warehouse,
        "database": database,
        "client_session_keep_alive": True,
    }
    if schema:
        params["schema"] = schema
    if role:
        params["role"] = role
    # Key-pair auth (service account)
    if key_path and os.path.isfile(key_path):
        from cryptography.hazmat.primitives.serialization import (
            Encoding,
            NoEncryption,
            PrivateFormat,
            load_pem_private_key,
        )
        from cryptography.hazmat.backends import default_backend

        passphrase = (os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or "").strip()
        with open(key_path, "rb") as f:
            private_key = load_pem_private_key(
                f.read(),
                password=passphrase.encode() if passphrase else None,
                backend=default_backend(),
            )
        params["private_key"] = private_key.private_bytes(
            Encoding.DER, PrivateFormat.PKCS8, NoEncryption()
        )
    elif password:
        params["password"] = password
    else:
        # Fallback — should not hit this with service account
        params["authenticator"] = os.getenv(
            "SNOWFLAKE_AUTHENTICATOR", "externalbrowser"
        )
    params["network_timeout"] = _env_int("SNOWFLAKE_NETWORK_TIMEOUT", 30)
    params["login_timeout"] = _env_int("SNOWFLAKE_LOGIN_TIMEOUT", 60)
    return params


def _create_snowflake_connection() -> Any:
    return snowflake.connector.connect(**_snowflake_conn_params())


def _init_pool() -> None:
    """Initialize pool with ``INITIAL_POOL_SIZE`` (thread-safe, once)."""
    global _pool_initialized, _pool_created_count
    if _pool_initialized:
        return
    with _pool_lock:
        if _pool_initialized:
            return
        log_debug(
            "Initializing Snowflake connection pool "
            f"({INITIAL_POOL_SIZE}/{POOL_SIZE} initial connections)..."
        )
        for _ in range(INITIAL_POOL_SIZE):
            _pool.put(_create_snowflake_connection())
            _pool_created_count += 1
        _pool_initialized = True
        log_debug(
            "✓ Snowflake pool ready "
            f"({_pool_created_count}/{POOL_SIZE} connections)"
        )


def return_connection(conn: Any) -> None:
    """Return a live connection to the pool, or replace it if dead."""
    if conn is None:
        return
    try:
        if not conn.is_closed():
            _pool.put_nowait(conn)
            return
    except Exception:
        pass
    log_debug("Replacing dead connection in pool")
    try:
        new_conn = _create_snowflake_connection()
        _pool.put_nowait(new_conn)
    except Exception as e:
        log_debug(f"Error replacing dead pool connection: {str(e)[:60]}")


def reset_snowflake_pool() -> None:
    """Drain pool, close connections, allow re-init (tests / hard recovery)."""
    global _pool_initialized, _pool_created_count
    with _pool_lock:
        _pool_initialized = False
        _pool_created_count = 0
        while True:
            try:
                c = _pool.get_nowait()
            except Empty:
                break
            try:
                c.close()
            except Exception:
                pass


def get_snowflake_connection() -> Any:
    """Borrow a connection from the pool (grow lazily up to ``POOL_SIZE``)."""
    _init_pool()
    # Grow on demand: avoid opening many SSO browser windows at startup.
    with _pool_lock:
        global _pool_created_count
        if _pool.empty() and _pool_created_count < POOL_SIZE:
            _pool.put(_create_snowflake_connection())
            _pool_created_count += 1
    try:
        return _pool.get(timeout=60)
    except Empty as e:
        raise RuntimeError(
            "Snowflake pool exhausted: no connection available within 60s"
        ) from e


def get_pdp_snowflake_connection() -> Any:
    """
    Get PDP Snowflake connection for DM_PRODUCT_PRD.GLD_ANALYTICS queries.

    On adoption branch: uses personal credentials (externalbrowser SSO)
    On other branches: uses service account key-pair auth

    Returns:
        Snowflake connection object
    """
    import subprocess

    # Check if we're on adoption branch
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=2,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        current_branch = result.stdout.strip()
        is_adoption_branch = (current_branch == "adoption")
    except Exception:
        # Can't determine branch - default to service account
        is_adoption_branch = False

    base_params = _snowflake_conn_params()

    # Override with PDP-specific database/schema/warehouse
    base_params["database"] = os.getenv("PDP_SNOWFLAKE_DATABASE", "DM_PRODUCT_PRD")
    base_params["schema"] = os.getenv("PDP_SNOWFLAKE_SCHEMA", "GLD_ANALYTICS")
    base_params["warehouse"] = os.getenv("PDP_SNOWFLAKE_WAREHOUSE", "DEMO_WH")

    if is_adoption_branch:
        # Use personal credentials with externalbrowser SSO
        base_params["user"] = os.getenv("PDP_SNOWFLAKE_USER", "smummadi@salesforce.com")
        base_params["role"] = os.getenv(
            "PDP_SNOWFLAKE_ROLE",
            "SNF_DM_PRODUCT_PRD_GLD_ANALYTICS_ANALYST_USR"
        )
        base_params["authenticator"] = "externalbrowser"
        # Remove private_key if present (force SSO)
        base_params.pop("private_key", None)
        base_params.pop("password", None)
        log_debug("[PDP] Using personal credentials (adoption branch)")
    else:
        # Use service account credentials (key-pair or password)
        # Role already set from base_params if PDP_SNOWFLAKE_ROLE not in env
        pdp_role = os.getenv("PDP_SNOWFLAKE_ROLE")
        if pdp_role:
            base_params["role"] = pdp_role
        log_debug("[PDP] Using service account credentials")

    try:
        conn = snowflake.connector.connect(**base_params)
        log_debug("[PDP] Snowflake connection established")
        return conn
    except Exception as e:
        log_error(f"[PDP] Connection failed: {e}")
        raise


def return_pdp_connection(conn: Any) -> None:
    """Close PDP connection (not pooled)."""
    if conn is None:
        return
    try:
        if not conn.is_closed():
            conn.close()
            log_debug("[PDP] Connection closed")
    except Exception as e:
        log_debug(f"[PDP] Error closing connection: {str(e)[:60]}")


def _product_atr_amount(p: dict) -> float:
    """ABS attrition pipeline from raw Snowflake row or normalized get_account_attrition dict."""
    try:
        if p.get("ATTRITION_PIPELINE") is not None:
            return abs(float(p.get("ATTRITION_PIPELINE") or 0))
        return abs(float(p.get("attrition") or 0))
    except (TypeError, ValueError):
        return 0.0


def _product_proba(p: dict) -> float:
    try:
        v = p.get("ATTRITION_PROBA")
        if v is None:
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def is_success_plan(product: dict) -> bool:
    """True if APM L2/L3 looks like a Success Plan offer (excluded from overall ARI)."""
    l2 = str(product.get("APM_LVL_2") or "").lower()
    l3 = str(product.get("APM_LVL_3") or "").lower()
    return any(kw in l2 or kw in l3 for kw in SUCCESS_PLAN_KEYWORDS)


def calculate_overall_ari(products: list, min_atr_threshold: float = 0) -> dict:
    """
    Account-level ARI: exclude Success Plans, optional ATR floor, then sort by ATR then probability.
    """
    core = [p for p in products if not is_success_plan(p)]

    if not core:
        return {
            "category": "Unknown",
            "probability": None,
            "reason": "No qualifying products (all Success Plans)",
            "top_product": None,
            "atr_amount": 0,
        }

    qualified = [p for p in core if _product_atr_amount(p) >= min_atr_threshold]
    if not qualified:
        qualified = list(core)

    qualified.sort(
        key=lambda x: (_product_atr_amount(x), _product_proba(x)),
        reverse=True,
    )
    top = qualified[0]
    product_name = (
        top.get("APM_LVL_3")
        or top.get("APM_LVL_2")
        or top.get("APM_LVL_1")
        or top.get("product")
        or "Unknown"
    )

    return {
        "category": top.get("ATTRITION_PROBA_CATEGORY")
        or top.get("category", "Unknown"),
        "probability": top.get("ATTRITION_PROBA"),
        "reason": top.get("ATTRITION_REASON") or top.get("reason") or "N/A",
        "top_product": product_name,
        "atr_amount": _product_atr_amount(top),
    }


def split_products_by_type(products: list) -> dict:
    """Split into core vs success-plan rows; sort each by ATR desc then probability desc."""
    core: list = []
    success_plans: list = []
    for p in products:
        if is_success_plan(p):
            success_plans.append(p)
        else:
            core.append(p)
    sort_key = lambda x: (_product_atr_amount(x), _product_proba(x))
    core.sort(key=sort_key, reverse=True)
    success_plans.sort(key=sort_key, reverse=True)
    return {"core": core, "success_plans": success_plans}


def run_query(
    sql: str,
    params: Optional[list] = None,
    *,
    statement_timeout: Optional[int] = None,
) -> list[dict]:
    """
    Execute Snowflake query using the connection pool; always returns the connection.

    ``statement_timeout``: seconds for ``STATEMENT_TIMEOUT_IN_SECONDS`` on this query
    only; when omitted, uses ``SNOWFLAKE_STATEMENT_TIMEOUT`` (default 30).
    """

    def _is_retryable_snowflake_cancel(err: BaseException) -> bool:
        # 000604 (57014): SQL execution canceled
        try:
            if not isinstance(err, snowflake.connector.errors.ProgrammingError):
                return False
            msg = str(err) or ""
            return "57014" in msg or "000604" in msg or "execution canceled" in msg.lower()
        except Exception:
            return False

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, min=2, max=4),
        retry=retry_if_exception(_is_retryable_snowflake_cancel),
        before_sleep=lambda rs: log_debug(
            f"[retry] Snowflake canceled/timeout; retrying ({rs.attempt_number}/2)..."
        ),
        reraise=True,
    )
    def _execute(conn: Any) -> list[dict]:
        cursor = conn.cursor()
        try:
            t_query_start = time.time()
            timeout = (
                statement_timeout
                if statement_timeout is not None
                else _env_int("SNOWFLAKE_STATEMENT_TIMEOUT", 30)
            )
            cursor.execute(
                f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout}"
            )
            cursor.execute(sql, params or [])
            rows = cursor.fetchall()
            elapsed = time.time() - t_query_start
            if elapsed > 5:
                log_structured(
                    "snowflake_slow_query",
                    level="warning",
                    latency_s=round(elapsed, 2),
                    row_count=len(rows),
                    sql_preview=str(sql)[:80],
                )
            if not cursor.description:
                return []
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        finally:
            cursor.close()

    def _is_connection_error(msg: str) -> bool:
        m = msg.lower()
        return any(
            kw in m
            for kw in (
                "connection",
                "session",
                "expired",
                "closed",
                "reset",
                "390114",
                "250002",
            )
        )

    conn = get_snowflake_connection()
    try:
        out = _execute(conn)
        return_connection(conn)
        return out
    except Exception as e:
        error_str = str(e)
        log_debug(f"Snowflake query error: {error_str[:100]}")
        if _is_connection_error(error_str):
            try:
                conn.close()
            except Exception:
                pass
            log_debug("Retrying Snowflake query with fresh connection...")
            conn2 = get_snowflake_connection()
            try:
                out = _execute(conn2)
                return_connection(conn2)
                return out
            except Exception as retry_e:
                log_debug(f"Snowflake retry failed: {str(retry_e)[:100]}")
                return_connection(conn2)
                raise
        return_connection(conn)
        raise


def to_15_char_id(account_id: str) -> str:
    if not account_id:
        return ""
    s = str(account_id)
    return s[:15] if len(s) > 15 else s


def apm_cloud_levels_predicate(cloud: str) -> str:
    """
    SQL predicate over APM_LVL_1/2/3 for CSS attrition.

    FSC: CIDM uses ``DRVD_APM_LVL_2 = 'Financial Services Cloud'`` under Industries; CSS
    uses ``APM_LVL_3`` like ``Financial Services Cloud - Sales`` / ``- Service``.
    Prefix match on ``Financial Services Cloud`` covers those; also match Industries at L2.
    Other clouds: full label plus first token (e.g. ``Commerce Cloud`` → ``Commerce``).
    """
    if not cloud or str(cloud).strip() == "" or str(cloud) == "All Clouds":
        return ""

    c = str(cloud).strip()
    c_low = c.lower()

    if "financial services" in c_low or c.upper() == "FSC":
        # Double %% — predicate is spliced into queries executed with pyformat (%s).
        return (
            "("
            "APM_LVL_3 LIKE '%%Financial Services Cloud%%' "
            "OR APM_LVL_2 LIKE '%%Financial Services%%' "
            "OR APM_LVL_1 LIKE '%%Financial Services%%' "
            "OR APM_LVL_2 LIKE '%%Industries%%'"
            ")"
        )

    c_safe = c.replace("'", "''").replace("%", "%%")
    variants: list[str] = [c_safe]
    first = c_safe.split(None, 1)[0] if c_safe else ""
    if first and first != c_safe and len(first) >= 3:
        variants.append(first)
    seen: set[str] = set()
    uniq: list[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            uniq.append(v)
    parts: list[str] = []
    for v in uniq:
        for col in ("APM_LVL_1", "APM_LVL_2", "APM_LVL_3"):
            parts.append(f"{col} LIKE '%%{v}%%'")
    return f"({' OR '.join(parts)})"


def get_cloud_filter(cloud: str) -> str:
    """APM cloud match for SQL embedded alongside %s placeholders — %% survives pyformat."""
    return apm_cloud_levels_predicate(cloud)


def fmt_amount(val) -> str:
    """
    Format dollar amount — M shorthand (Option A).
    $695,492 → $0.7M ; $1,608,311 → $1.6M ; $0 → $0
    """
    try:
        num = float(val)
        if num == 0:
            return "$0"
        elif abs(num) >= 1_000:
            return f"${num / 1_000_000:.1f}M"
        else:
            return f"${num:.0f}"
    except (TypeError, ValueError):
        return str(val) if val else "N/A"


def get_usage_unified(account_id: str | list[str], cloud: str | None = None) -> dict:
    """
    Unified usage fetch: summary stats and raw rows from CIDM.

    Runs a lightweight ``SELECT 1 … LIMIT 1`` pre-check first. If the account has no
    CIDM rows with PROVISIONED > 0, returns empty immediately instead of three
    sequential fallbacks.

    Returns:
        ``{"summary": {...}, "raw_rows": [...]}`` — summary has utilization / source / emoji;
        ``raw_rows`` are CIDM usage rows (for ``build_adoption_pov``).

    Multi-account (hierarchy): by default merges per-account CIDM queries (set
    ``SNOWFLAKE_USAGE_HIERARCHY_USE_AGGREGATE=1`` for one ``GROUP BY`` over all ids).
    """
    CLOUD_L1_MAP = {
        # Commerce
        "Commerce Cloud": "Commerce",
        "B2C Commerce": "Commerce",
        "B2B Commerce": "Commerce",
        # FSC — under Industries in CIDM; ``DRVD_APM_LVL_2`` is ``Financial Services Cloud``
        "Financial Services Cloud": "Financial Services Cloud",
        "FSC": "Financial Services Cloud",
        # Marketing
        "Marketing Cloud": "Marketing",
        # Sales
        "Sales Cloud": "Sales",
        # Service
        "Service Cloud": "Service",
        # Data
        "Data Cloud": "AI and Data",
        # Analytics
        "Tableau": "Analytics",
        # Integration
        "MuleSoft": "Integration",
        # Industries (parent bucket for FSC, etc.)
        "Industries": "Industries",
        "Health Cloud": "Health Cloud",
    }

    def _build_cloud_filter(cloud_val: str | None) -> str:
        if not cloud_val or cloud_val == "All Clouds":
            return ""
        # FSC: CIDM usage is typically under Industries (L1) but the signal we want is
        # exact L2 = 'Financial Services Cloud'. This dramatically reduces scan size
        # and matches build_adoption_pov()’s FSC logic.
        c_norm = str(cloud_val).strip().lower()
        if "financial services" in c_norm or c_norm == "fsc":
            return """
                AND DRVD_APM_LVL_2 = 'Financial Services Cloud'
            """
        l1_value = CLOUD_L1_MAP.get(str(cloud_val).strip(), cloud_val)
        l1_safe = str(l1_value).replace("'", "''").replace("%", "%%")
        return f"""
            AND (
                DRVD_APM_LVL_1 LIKE '%%{l1_safe}%%'
                OR DRVD_APM_LVL_2 LIKE '%%{l1_safe}%%'
            )
        """

    cloud_filter = _build_cloud_filter(cloud)

    def _hierarchy_summary_from_rows(merged_rows: list) -> dict:
        if not merged_rows:
            return {"summary": {}, "raw_rows": []}
        gmv_rows = [
            r for r in merged_rows if str(r.get("GRP", "")).upper() == "GMV"
        ]
        if gmv_rows:
            total_prov = sum(float(r.get("PROVISIONED") or 0) for r in gmv_rows)
            total_used = sum(float(r.get("USED") or 0) for r in gmv_rows)
            source = "GMV"
        else:
            commerce_rows = [
                r
                for r in merged_rows
                if "commerce" in str(r.get("DRVD_APM_LVL_1", "")).lower()
                or "commerce" in str(r.get("DRVD_APM_LVL_2", "")).lower()
            ]
            target_rows = commerce_rows if commerce_rows else merged_rows
            total_prov = sum(
                float(r.get("PROVISIONED") or 0) for r in target_rows
            )
            total_used = sum(float(r.get("USED") or 0) for r in target_rows)
            source = "Commerce aggregate" if commerce_rows else "All products"
        if total_prov > 0:
            util_rate = (total_used / total_prov) * 100
            util_str = f"{util_rate:.1f}%"
        else:
            util_rate = 0
            util_str = "N/A"
        if util_rate >= 70:
            util_emoji = ":large_green_circle:"
        elif util_rate >= 40:
            util_emoji = ":large_yellow_circle:"
        elif util_rate > 0:
            util_emoji = ":red_circle:"
        else:
            util_emoji = ":white_circle:"
        return {
            "summary": {
                "utilization_rate": util_str,
                "util_emoji": util_emoji,
                "cloud_aov": "Unknown",
                "gmv_util": util_str if gmv_rows else None,
                "source": source,
            },
            "raw_rows": merged_rows,
        }

    # Support parent + child hierarchy usage in one call.
    if isinstance(account_id, (list, tuple, set)):
        # Single query for hierarchy accounts (avoid N sequential Snowflake calls).
        account_ids = [
            to_15_char_id(str(aid))
            for aid in account_id
            if str(aid or "").strip()
        ]
        account_ids = list(dict.fromkeys(account_ids))
        if not account_ids:
            return {"summary": {}, "raw_rows": []}

        if len(account_ids) == 1:
            return get_usage_unified(account_ids[0], cloud)

        # Multi-account: prefer sequential per-account CIDM (ACCOUNT_ID = %s). Heavy
        # ``IN (...)`` + ``GROUP BY`` often hits stricter warehouse statement caps (~30s)
        # even when the session requests a longer timeout.
        def _hierarchy_per_account_merge_with_batch_precheck(log_msg: str) -> dict:
            log_debug(log_msg)
            pinned_pc = (os.getenv("SNOWFLAKE_CIDM_SNAPSHOT_DT") or "").strip()
            if not pinned_pc:
                pinned_val = _usage_snapshot_cache.get(
                    "cidm_wv_av_usage_extract_vw_max_snapshot_dt"
                )
                pinned_pc = str(pinned_val) if pinned_val else ""

            placeholders = ", ".join(["%s"] * len(account_ids))
            try:
                if pinned_pc:
                    existing_rows = run_query(
                        f"""
                        SELECT DISTINCT ACCOUNT_ID
                        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
                        WHERE ACCOUNT_ID IN ({placeholders})
                        AND SNAPSHOT_DT = %s
                        AND PROVISIONED > 0
                        """,
                        [*account_ids, pinned_pc],
                        statement_timeout=_env_int(
                            "SNOWFLAKE_USAGE_PRECHECK_TIMEOUT", 20
                        ),
                    )
                else:
                    existing_rows = run_query(
                        f"""
                        SELECT DISTINCT ACCOUNT_ID
                        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
                        WHERE ACCOUNT_ID IN ({placeholders})
                        AND CURR_SNAP_FLG = 'Y'
                        AND PROVISIONED > 0
                        """,
                        list(account_ids),
                        statement_timeout=_env_int(
                            "SNOWFLAKE_USAGE_PRECHECK_TIMEOUT", 20
                        ),
                    )
                existing_ids = {r["ACCOUNT_ID"] for r in existing_rows}
                log_debug(
                    f"get_usage_unified hierarchy batch pre-check: "
                    f"{len(existing_ids)}/{len(account_ids)} accounts have CIDM data"
                )
            except Exception as e:
                log_debug(
                    f"get_usage_unified hierarchy batch pre-check error: "
                    f"{_fmt_exc(e)[:80]}"
                )
                existing_ids = set(account_ids)

            merged_local: list = []
            for aid in account_ids:
                if aid not in existing_ids:
                    log_debug(
                        f"get_usage_unified: no CIDM data for {aid}, "
                        "skipping (batch pre-check)"
                    )
                    continue
                try:
                    part = get_usage_unified(aid, cloud).get("raw_rows") or []
                    merged_local.extend(part)
                except Exception as e:
                    log_debug(
                        f"get_usage_unified hierarchy per-account {aid}: "
                        f"{_fmt_exc(e)[:80]}"
                    )
            return _hierarchy_summary_from_rows(merged_local)

        use_hierarchy_aggregate = _env_int(
            "SNOWFLAKE_USAGE_HIERARCHY_USE_AGGREGATE", 0
        )
        if not use_hierarchy_aggregate:
            return _hierarchy_per_account_merge_with_batch_precheck(
                "get_usage_unified hierarchy: per-account merge only "
                f"({len(account_ids)} ids; set SNOWFLAKE_USAGE_HIERARCHY_USE_AGGREGATE=1 "
                "for single GROUP BY)"
            )

        # Hierarchy aggregate (opt-in): CURR_SNAP / scoped MAX — same strategy as single-account.
        ph = ", ".join(["%s"] * len(account_ids))
        cidm_to = _env_int(
            "SNOWFLAKE_USAGE_CIDM_HIERARCHY_TIMEOUT",
            max(120, _env_int("SNOWFLAKE_USAGE_CIDM_TIMEOUT", 45)),
        )
        pinned = (os.getenv("SNOWFLAKE_CIDM_SNAPSHOT_DT") or "").strip()

        def _hierarchy_agg(snap_fragment: str, bind: list, cf: str) -> list:
            sql = f"""
            SELECT
                ACCOUNT_ID,
                DRVD_APM_LVL_1,
                DRVD_APM_LVL_2,
                GRP,
                TYPE,
                SUM(PROVISIONED) AS PROVISIONED,
                SUM(ACTIVATED) AS ACTIVATED,
                SUM(USED) AS USED
            FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
            WHERE ACCOUNT_ID IN ({ph})
            {snap_fragment}
            {cf}
            AND PROVISIONED > 0
            GROUP BY ACCOUNT_ID, DRVD_APM_LVL_1, DRVD_APM_LVL_2, GRP, TYPE
            ORDER BY PROVISIONED DESC
            """
            try:
                return run_query(sql, bind, statement_timeout=cidm_to)
            except Exception as e:
                log_debug(
                    f"get_usage_unified hierarchy aggregate error: {str(e)[:100]}"
                )
                return []

        merged_rows: list = []
        if pinned:
            merged_rows = _hierarchy_agg(
                "AND SNAPSHOT_DT = %s",
                [*account_ids, pinned],
                cloud_filter,
            )
        if not merged_rows:
            merged_rows = _hierarchy_agg(
                "AND CURR_SNAP_FLG = 'Y'",
                list(account_ids),
                cloud_filter,
            )
        if not merged_rows:
            scoped_max = (
                "AND SNAPSHOT_DT = ("
                "SELECT MAX(SNAPSHOT_DT) "
                "FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW "
                f"WHERE ACCOUNT_ID IN ({ph}))"
            )
            merged_rows = _hierarchy_agg(
                scoped_max,
                [*account_ids, *account_ids],
                cloud_filter,
            )
        if not merged_rows:
            merged_rows = _hierarchy_agg(
                "AND CURR_SNAP_FLG = 'Y'",
                list(account_ids),
                "",
            )
        if not merged_rows:
            scoped_max = (
                "AND SNAPSHOT_DT = ("
                "SELECT MAX(SNAPSHOT_DT) "
                "FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW "
                f"WHERE ACCOUNT_ID IN ({ph}))"
            )
            merged_rows = _hierarchy_agg(
                scoped_max,
                [*account_ids, *account_ids],
                "",
            )
        if not merged_rows:
            try:
                merged_rows = run_query(
                    f"""
                    SELECT
                        ACCOUNT_ID,
                        DRVD_APM_LVL_1,
                        DRVD_APM_LVL_2,
                        GRP,
                        TYPE,
                        SUM(PROVISIONED) AS PROVISIONED,
                        SUM(ACTIVATED) AS ACTIVATED,
                        SUM(USED) AS USED
                    FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
                    WHERE ACCOUNT_ID IN ({ph})
                    AND CURR_SNAP_FLG = 'Y'
                    AND GRP = 'GMV'
                    AND PROVISIONED > 0
                    GROUP BY ACCOUNT_ID, DRVD_APM_LVL_1, DRVD_APM_LVL_2, GRP, TYPE
                    ORDER BY PROVISIONED DESC
                    """,
                    list(account_ids),
                    statement_timeout=cidm_to,
                )
            except Exception as e:
                log_debug(f"get_usage_unified hierarchy GMV error: {str(e)[:80]}")
                merged_rows = []
        if not merged_rows:
            return _hierarchy_per_account_merge_with_batch_precheck(
                "get_usage_unified hierarchy: per-account CIDM merge "
                f"(aggregate fallbacks exhausted, {len(account_ids)} ids)"
            )
        return _hierarchy_summary_from_rows(merged_rows)

    account_id = to_15_char_id(str(account_id))

    # Fast pre-check: skip CURR_SNAP / MAX(SNAPSHOT) / GMV fallbacks when no CIDM usage rows
    try:
        check = run_query(
            """
            SELECT 1 AS EXISTS_FLAG
            FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
            WHERE ACCOUNT_ID = %s
            AND PROVISIONED > 0
            LIMIT 1
            """,
            [account_id],
            statement_timeout=_env_int("SNOWFLAKE_USAGE_PRECHECK_TIMEOUT", 20),
        )
        if not check:
            log_debug(
                f"get_usage_unified: no CIDM data for {account_id}, skipping fallbacks"
            )
            return {"summary": {}, "raw_rows": []}
    except Exception as e:
        log_debug(f"get_usage_unified pre-check error: {str(e)[:60]}")
        # Continue with normal fallback chain if pre-check fails

    cidm_timeout = _env_int("SNOWFLAKE_USAGE_CIDM_TIMEOUT", 45)

    def _run(
        snap_filter: str,
        params: list,
        *,
        statement_timeout: int | None = None,
    ) -> list:
        sql = f"""
            SELECT
                ACCOUNT_ID,
                DRVD_APM_LVL_1,
                DRVD_APM_LVL_2,
                GRP,
                TYPE,
                PROVISIONED,
                ACTIVATED,
                USED
            FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
            WHERE ACCOUNT_ID = %s
            {snap_filter}
            {cloud_filter}
            AND PROVISIONED > 0
            ORDER BY PROVISIONED DESC
        """
        try:
            if statement_timeout is not None:
                return run_query(sql, params, statement_timeout=statement_timeout)
            return run_query(sql, params)
        except Exception as e:
            log_debug(f"get_usage_unified error: {str(e)[:100]}")
            return []

    rows = _run(
        "AND CURR_SNAP_FLG = 'Y'",
        [account_id],
        statement_timeout=cidm_timeout,
    )

    if not rows:
        log_debug(
            "get_usage_unified: CURR_SNAP_FLG=Y returned nothing, trying MAX(SNAPSHOT_DT)"
        )
        rows = _run(
            """
            AND SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
                WHERE ACCOUNT_ID = %s
            )
            """,
            [account_id, account_id],
            statement_timeout=cidm_timeout,
        )

    if not rows:
        log_debug("get_usage_unified: trying without cloud filter, GMV only")
        try:
            rows = run_query(
                """
                SELECT
                    ACCOUNT_ID,
                    DRVD_APM_LVL_1, DRVD_APM_LVL_2,
                    GRP, TYPE,
                    PROVISIONED, ACTIVATED, USED
                FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
                WHERE ACCOUNT_ID = %s
                AND CURR_SNAP_FLG = 'Y'
                AND GRP = 'GMV'
                AND PROVISIONED > 0
                ORDER BY PROVISIONED DESC
                """,
                [account_id],
                statement_timeout=cidm_timeout,
            )
        except Exception as e:
            log_debug(f"GMV fallback error: {str(e)[:100]}")

    if not rows:
        return {"summary": {}, "raw_rows": []}

    gmv_rows = [r for r in rows if str(r.get("GRP", "")).upper() == "GMV"]

    if gmv_rows:
        total_prov = sum(float(r.get("PROVISIONED") or 0) for r in gmv_rows)
        total_used = sum(float(r.get("USED") or 0) for r in gmv_rows)
        source = "GMV"
    else:
        commerce_rows = [
            r
            for r in rows
            if "commerce" in str(r.get("DRVD_APM_LVL_1", "")).lower()
            or "commerce" in str(r.get("DRVD_APM_LVL_2", "")).lower()
        ]
        target_rows = commerce_rows if commerce_rows else rows
        total_prov = sum(float(r.get("PROVISIONED") or 0) for r in target_rows)
        total_used = sum(float(r.get("USED") or 0) for r in target_rows)
        source = "Commerce aggregate" if commerce_rows else "All products"

    if total_prov > 0:
        util_rate = (total_used / total_prov) * 100
        util_str = f"{util_rate:.1f}%"
    else:
        util_rate = 0
        util_str = "N/A"

    log_debug(
        f"✓ Usage ({source}): {util_str} util, prov={total_prov:,.0f}, used={total_used:,.0f}"
    )

    if util_rate >= 70:
        util_emoji = ":large_green_circle:"
    elif util_rate >= 40:
        util_emoji = ":large_yellow_circle:"
    elif util_rate > 0:
        util_emoji = ":red_circle:"
    else:
        util_emoji = ":white_circle:"

    summary = {
        "utilization_rate": util_str,
        "util_emoji": util_emoji,
        "cloud_aov": "Unknown",
        "gmv_util": util_str if gmv_rows else None,
        "source": source,
    }

    return {"summary": summary, "raw_rows": rows}


def extract_usd(value) -> float:
    """Extract numeric USD value from various formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").strip()
        if not cleaned or cleaned.lower() in ("unknown", "n/a"):
            return 0.0
        m_suffix = cleaned[-1].upper() == "M" and len(cleaned) > 1
        if m_suffix:
            cleaned = cleaned[:-1].strip()
        try:
            n = float(cleaned)
            return n * 1_000_000 if m_suffix else n
        except ValueError:
            return 0.0
    return 0.0


def _renewal_dict(snapshot: Optional[dict]) -> dict:
    r = (snapshot or {}).get("renewal_aov")
    return r if isinstance(r, dict) else {}


def resolve_money(snowflake_display: dict, opp: dict, field: str) -> str:
    """
    Resolve money fields with Snowflake-first, org62 fallback.
    field: one of "atr", "attrition", "aov", "swing".
    """
    disp = snowflake_display or {}
    ren = _renewal_dict(disp)

    if field in ("atr", "attrition"):
        val = 0.0
        if opp:
            val = extract_usd(opp.get("Forecasted_Attrition__c"))
        if not val:
            v2 = disp.get("renewal_atr")
            if v2 is None:
                v2 = ren.get("renewal_atr_snow")
            if v2 is not None:
                try:
                    val = float(v2)
                except (TypeError, ValueError):
                    val = 0.0
        if val:
            val = abs(float(val))
        return fmt_amount(val) if val else "N/A"

    if field == "aov":
        val = ren.get("renewal_aov") if ren else None
        if val is not None:
            try:
                val = float(val)
            except (TypeError, ValueError):
                val = 0.0
        if not val:
            val = extract_usd(disp.get("cc_aov"))
        if not val and opp:
            val = extract_usd(opp.get("Amount"))
        return fmt_amount(val) if val else "N/A"

    if field == "swing":
        val = extract_usd(opp.get("Swing__c")) if opp else 0.0
        if not val:
            v2 = ren.get("renewal_swing_snow")
            if v2 is not None:
                try:
                    val = abs(float(v2))
                except (TypeError, ValueError):
                    val = 0.0
        return fmt_amount(val) if val else "N/A"

    return "N/A"


def enrich_account(
    account_id,
    opty_id=None,
    cloud=None,
    usage_account_ids: Optional[list[str]] = None,
    renewal_prefetch: Optional[dict] = None,
):
    """
    Full enrichment with parallel I/O.

    Phase 1: independent queries run concurrently (health + usage + renewal AOV + opp ARI
    when ``opty_id`` is set). ``max_workers=4`` runs all four without queuing in this pool.

    If ``renewal_prefetch`` is set (same shape as ``get_renewal_aov``), skips the
    Snowflake renewal-view lookup for AOV/ATR/geo and uses the preloaded row.

    Phase 2: ARI fallback (account-level) only if opp-level ARI is Unknown.

    Phase 3: ``cloud_aov`` from ``renewal_aov`` if still Unknown.
    """
    start = time.time()
    account_id_15 = to_15_char_id(account_id)
    css_skip = str(os.getenv("SNOWFLAKE_CSS_SKIP") or "").strip() in ("1", "true", "yes", "on")

    result = {
        "ari": {
            "probability": None,
            "category": "Unknown",
            "reason": "N/A",
        },
        "renewal_aov": {},
        "health": {"overall_score": 0, "overall_literal": "Unknown"},
        "usage": {
            "utilization_rate": "N/A",
            "util_emoji": ":white_circle:",
            "cloud_aov": "Unknown",
            "gmv_util": None,
            "source": "",
        },
        "usage_raw_rows": [],
    }

    if renewal_prefetch:
        result["renewal_aov"] = dict(renewal_prefetch)

    # Up to four Snowflake calls (health, usage, renewal AOV, opp ARI).
    # Multi-account hierarchy usage runs a heavy CIDM aggregation; run phase 1 **serially**
    # so we do not stack 4 Snowflake queries on top of GM Review's parallel attrition/red.
    _tw_health = _env_int("SNOWFLAKE_ENRICH_HEALTH_WAIT_S", 90)
    _tw_usage = _env_int("SNOWFLAKE_ENRICH_USAGE_WAIT_S", 120)
    _tw_renew = _env_int("SNOWFLAKE_ENRICH_RENEWAL_WAIT_S", 45)
    _tw_ari = _env_int("SNOWFLAKE_ENRICH_ARI_WAIT_S", 45)

    usage_target: str | list[str] = (
        usage_account_ids if usage_account_ids else account_id_15
    )
    _hierarchy_usage = isinstance(usage_target, list) and len(usage_target) > 1

    def _phase1_health() -> None:
        try:
            health_data = get_customer_health(account_id_15)
            if health_data:
                result["health"] = health_data
        except Exception as e:
            log_debug(f"Health fetch error: {_fmt_exc(e)[:120]}")

    def _phase1_usage() -> None:
        try:
            usage_unified = get_usage_unified(usage_target, cloud)
            if usage_unified and usage_unified.get("summary"):
                usage_data = usage_unified["summary"]
                result["usage"] = {
                    "utilization_rate": usage_data.get("utilization_rate", "N/A"),
                    "util_emoji": usage_data.get("util_emoji", ":white_circle:"),
                    "cloud_aov": usage_data.get("cloud_aov", "Unknown"),
                    "gmv_util": usage_data.get("gmv_util"),
                    "source": usage_data.get("source", ""),
                }
                result["usage_raw_rows"] = usage_unified.get("raw_rows", [])
                log_debug(
                    f"✓ Usage: {usage_data.get('utilization_rate')} "
                    f"({usage_data.get('source')})"
                )
        except Exception as e:
            log_debug(f"Usage fetch error: {_fmt_exc(e)[:120]}")

    def _phase1_renew() -> None:
        try:
            if not renewal_prefetch and opty_id:
                renewal_data = get_renewal_aov(opty_id)
                if renewal_data:
                    result["renewal_aov"] = renewal_data
                    if result["usage"].get("cloud_aov") == "Unknown":
                        result["usage"]["cloud_aov"] = fmt_amount(
                            renewal_data.get("renewal_aov", 0)
                        )
            elif renewal_prefetch and result["usage"].get("cloud_aov") == "Unknown":
                result["usage"]["cloud_aov"] = fmt_amount(
                    renewal_prefetch.get("renewal_aov", 0)
                )
        except Exception as e:
            log_debug(f"Renewal AOV error: {str(e)[:60]}")

    def _phase1_ari() -> None:
        try:
            if opty_id:
                ari_data = get_ari_score(opty_id)
                if ari_data:
                    result["ari"] = {
                        "probability": ari_data.get("ATTRITION_PROBA"),
                        "category": ari_data.get(
                            "ATTRITION_PROBA_CATEGORY", "Unknown"
                        ),
                        "reason": ari_data.get("ATTRITION_REASON", "N/A"),
                    }
        except Exception as e:
            log_debug(f"ARI fetch error: {str(e)[:60]}")

    if _hierarchy_usage:
        if not css_skip:
            _phase1_health()
        _phase1_usage()
        _phase1_renew()
        if not css_skip:
            _phase1_ari()
    else:
        with ThreadPoolExecutor(max_workers=4) as ex:
            fut_health = ex.submit(get_customer_health, account_id_15) if not css_skip else None
            fut_usage = ex.submit(get_usage_unified, usage_target, cloud)
            fut_aov = (
                ex.submit(get_renewal_aov, opty_id)
                if (not renewal_prefetch and opty_id)
                else None
            )
            fut_ari = ex.submit(get_ari_score, opty_id) if (opty_id and not css_skip) else None

            try:
                if fut_health is not None:
                    health_data = fut_health.result(timeout=_tw_health)
                    if health_data:
                        result["health"] = health_data
            except Exception as e:
                log_debug(f"Health fetch error: {_fmt_exc(e)[:120]}")

            try:
                usage_unified = fut_usage.result(timeout=_tw_usage)
                if usage_unified and usage_unified.get("summary"):
                    usage_data = usage_unified["summary"]
                    result["usage"] = {
                        "utilization_rate": usage_data.get(
                            "utilization_rate", "N/A"
                        ),
                        "util_emoji": usage_data.get(
                            "util_emoji", ":white_circle:"
                        ),
                        "cloud_aov": usage_data.get("cloud_aov", "Unknown"),
                        "gmv_util": usage_data.get("gmv_util"),
                        "source": usage_data.get("source", ""),
                    }
                    result["usage_raw_rows"] = usage_unified.get("raw_rows", [])
                    log_debug(
                        f"✓ Usage: {usage_data.get('utilization_rate')} "
                        f"({usage_data.get('source')})"
                    )
            except Exception as e:
                log_debug(f"Usage fetch error: {_fmt_exc(e)[:120]}")

            try:
                if fut_aov:
                    renewal_data = fut_aov.result(timeout=_tw_renew)
                    if renewal_data:
                        result["renewal_aov"] = renewal_data
                        if result["usage"].get("cloud_aov") == "Unknown":
                            result["usage"]["cloud_aov"] = fmt_amount(
                                renewal_data.get("renewal_aov", 0)
                            )
                elif (
                    renewal_prefetch
                    and result["usage"].get("cloud_aov") == "Unknown"
                ):
                    result["usage"]["cloud_aov"] = fmt_amount(
                        renewal_prefetch.get("renewal_aov", 0)
                    )
            except Exception as e:
                log_debug(f"Renewal AOV error: {str(e)[:60]}")

            try:
                if fut_ari:
                    ari_data = fut_ari.result(timeout=_tw_ari)
                    if ari_data:
                        result["ari"] = {
                            "probability": ari_data.get("ATTRITION_PROBA"),
                            "category": ari_data.get(
                                "ATTRITION_PROBA_CATEGORY", "Unknown"
                            ),
                            "reason": ari_data.get("ATTRITION_REASON", "N/A"),
                        }
            except Exception as e:
                log_debug(f"ARI fetch error: {str(e)[:60]}")

    if (not css_skip) and result["ari"]["category"] == "Unknown":
        try:
            att = get_account_attrition_all_cached(account_id_15)
            all_products = filter_products_by_cloud(att.get("all", []), cloud)
            if all_products:
                ari_result = calculate_overall_ari(
                    all_products, min_atr_threshold=0
                )
                result["ari"] = {
                    "probability": ari_result["probability"],
                    "category": ari_result["category"],
                    "reason": ari_result["reason"],
                    "top_product": ari_result.get("top_product"),
                    "atr_amount": ari_result.get("atr_amount", 0),
                }
                log_debug(
                    "ARI from account-level: "
                    f"{ari_result['category']} via "
                    f"{ari_result.get('top_product')}"
                )
        except Exception as e:
            log_debug(f"ARI account-level error: {str(e)[:60]}")

    if (
        result["usage"].get("cloud_aov") == "Unknown"
        and result.get("renewal_aov")
    ):
        result["usage"]["cloud_aov"] = fmt_amount(
            result["renewal_aov"].get("renewal_aov", 0)
        )

    log_debug(f"✓ enrich_account took {time.time() - start:.2f}s")
    return result


def enrich_account_cached(account_id, opty_id=None, cloud=None, **kwargs):
    key = f"{to_15_char_id(account_id)}|{opty_id or ''}|{cloud or ''}"
    cached = _cache_get(_account_enrich_cache, key)
    if cached is not None:
        log_debug(f"enrich_account cache HIT: {account_id}")
        return cached
    result = enrich_account(account_id, opty_id, cloud, **kwargs)
    _cache_set(_account_enrich_cache, key, result)
    return result


def get_ari_score(opty_id):
    """Get ARI for specific opportunity - CORRECTED: Use 15-char ID and MAX(SNAPSHOT_DT)"""
    if str(os.getenv("SNOWFLAKE_CSS_SKIP") or "").strip() in ("1", "true", "yes", "on"):
        return None
    if not opty_id:
        return None
    opty_id_15 = to_15_char_id(opty_id)
    sql = """
        SELECT ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
               ATTRITION_REASON
        FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_OPPTY
        WHERE RENEWAL_OPTY_ID = %s
        AND SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_OPPTY)
        LIMIT 1
    """
    rows = run_query(
        sql,
        [opty_id_15],
        statement_timeout=_env_int("SNOWFLAKE_RENEWAL_STATEMENT_TIMEOUT", 120),
    )
    return rows[0] if rows else None


def get_ari_score_by_account(account_id: str, cloud: str | None = "Commerce Cloud") -> list:
    """ARI rows for account on latest CSS ATTRITION_PREDICTION_ACCT_PRODUCT snapshot."""
    if str(os.getenv("SNOWFLAKE_CSS_SKIP") or "").strip() in ("1", "true", "yes", "on"):
        return []
    base = [
        "ACCOUNT_ID = %s",
        "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM "
        "SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT)",
    ]
    params: list[Any] = [account_id]

    def _run(with_cloud: str | None) -> list:
        cond = list(base)
        if with_cloud:
            pred = apm_cloud_levels_predicate(with_cloud)
            if pred:
                cond.append(pred)
        sql = f"""
            SELECT ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
                   ATTRITION_REASON, APM_LVL_2, APM_LVL_3
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE {' AND '.join(cond)}
            ORDER BY ATTRITION_PROBA DESC NULLS LAST
            LIMIT 5
        """
        return run_query(sql, params)

    use_cloud = cloud and str(cloud).strip() and str(cloud) != "All Clouds"
    rows = _run(cloud if use_cloud else None)
    if not rows and use_cloud:
        log_debug("get_ari_score_by_account: no rows with cloud filter; retrying without")
        rows = _run(None)
    return rows


def get_customer_health(account_id):
    """
    Fetch customer health score.

    ``CI_CH_FACT_CUSTOMER_HEALTH_VW`` has no ``CURR_SNAP_FLG``; latest snap is
    ``MAX(SNAPSHOT_DT)`` scoped to this ``ACCOUNT_ID`` in the subquery (avoids a
    full-table max). Literals are normalized to Green/Yellow/Red/Unknown below.
    """
    if str(os.getenv("SNOWFLAKE_CSS_SKIP") or "").strip() in ("1", "true", "yes", "on"):
        return {}
    sql = """
        SELECT CATEGORY, SUB_CATEGORY,
               OVERALL_SCORE, CATEGORY_SCORE,
               OVERALL_LITERAL_SCORE, CATEGORY_LITERAL_SCORE
        FROM SSE_DM_CSG_RPT_PRD.CSS.CI_CH_FACT_CUSTOMER_HEALTH_VW
        WHERE ACCOUNT_ID = %s
        AND SNAPSHOT_DT = (
            SELECT MAX(SNAPSHOT_DT)
            FROM SSE_DM_CSG_RPT_PRD.CSS.CI_CH_FACT_CUSTOMER_HEALTH_VW
            WHERE ACCOUNT_ID = %s
        )
        ORDER BY CATEGORY
        LIMIT 20
    """
    rows = run_query(
        sql,
        [account_id, account_id],
        statement_timeout=_env_int("SNOWFLAKE_HEALTH_STATEMENT_TIMEOUT", 90),
    )

    if not rows:
        return None

    def _normalize_literal(val, score=None) -> str:
        """
        Normalize health literal to Green/Yellow/Red/Unknown.
        CSS sometimes returns numeric string (e.g. '67') instead of label.
        Falls back to score-based band if literal is empty.
        """
        if val is None or str(val).strip() == "":
            if score is not None:
                try:
                    s = float(score)
                    if s >= 70:
                        return "Green"
                    if s >= 40:
                        return "Yellow"
                    return "Red"
                except (TypeError, ValueError):
                    pass
            return "Unknown"

        try:
            s = float(val)
            if s >= 70:
                return "Green"
            if s >= 40:
                return "Yellow"
            return "Red"
        except (TypeError, ValueError):
            pass

        label = str(val).strip().title()
        if label in ("Green", "Yellow", "Red"):
            return label
        return "Unknown"

    overall_score = rows[0].get("OVERALL_SCORE")
    overall_literal_raw = rows[0].get("OVERALL_LITERAL_SCORE")
    overall_literal = _normalize_literal(overall_literal_raw, overall_score)

    return {
        "overall_score": overall_score,
        "overall_literal": overall_literal,
        "categories": [
            {
                "category": r.get("CATEGORY"),
                "score": r.get("CATEGORY_SCORE"),
                "literal": _normalize_literal(
                    r.get("CATEGORY_LITERAL_SCORE"),
                    r.get("CATEGORY_SCORE"),
                ),
            }
            for r in rows
        ],
    }


_GMV_RATE_COLUMN_PREFERENCE: tuple[str, ...] = (
    "RENEWAL_GMV_UTIL_PCT",
    "RENEWAL_GMV_UTILIZATION_PCT",
    "GMV_UTILIZATION_RATE",
    "RENEWAL_GMV_RATE_PCT",
    "GMV_RATE_PCT",
    "RENEWAL_GMV_RATE",
    "GMV_RATE",
    "RENEWAL_GMV_UTLZTN_PCT",
    "GMV_UTLZN_RATE",
)


def _format_gmv_rate_for_display(val: Any) -> Optional[str]:
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        s = str(val).strip()
        return s if s else None
    if 0 <= f <= 1.0:
        return f"{f * 100:.1f}%"
    return f"{f:.1f}%"


def _gmv_rate_pct_from_renewal_row(row: dict) -> Optional[str]:
    """Pick GMV rate from a renewal snap row; prefers known column names."""
    if not row:
        return None
    for key in _GMV_RATE_COLUMN_PREFERENCE:
        if key in row and row[key] is not None:
            out = _format_gmv_rate_for_display(row[key])
            if out:
                return out
    for k, v in row.items():
        if v is None:
            continue
        ku = str(k).upper()
        if "GMV" in ku and any(
            x in ku for x in ("RATE", "UTIL", "PCT", "UTLZ", "BURN")
        ):
            out = _format_gmv_rate_for_display(v)
            if out:
                return out
    return None


def get_renewal_aov(opty_id):
    """Renewal row from ``WV_CI_RENEWAL_OPTY_SNAP_VW`` (explicit columns; pinned AS_OF_DATE)."""
    if not opty_id:
        return {}
    opty_id_15 = to_15_char_id(opty_id)
    renewal_view = _get_renewal_view()
    pinned = (os.getenv("SNOWFLAKE_RENEWAL_AS_OF_DATE") or "").strip()
    if not pinned:
        pv = _usage_snapshot_cache.get("renewal_as_of_date")
        pinned = str(pv).strip() if pv is not None else ""

    if pinned and renewal_view.endswith("WV_CI_RENEWAL_OPTY_SNAP_VW"):
        as_of_fragment = "AND AS_OF_DATE = %s"
        as_of_bind = [pinned]
    elif renewal_view.endswith("WV_CI_RENEWAL_OPTY_SNAP_VW"):
        as_of_fragment = """AND AS_OF_DATE = (
            SELECT MAX(AS_OF_DATE)
            FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_SNAP_VW
        )"""
        as_of_bind = []
    else:
        as_of_fragment = ""
        as_of_bind = []

    sql = f"""
        SELECT
            RENEWAL_OPTY_ID,
            RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV,
            RENEWAL_FCAST_ATTRITION_CONV,
            RENEWAL_ATR_CONV,
            CONV_SWING_AMT,
            RENEWAL_FCAST_CODE,
            RENEWAL_STG_NM,
            RENEWAL_KEY_RISK_CAT,
            RENEWAL_RISK_DETAIL,
            RENEWAL_CLSD_DT,
            RENEWAL_CLOSE_MONTH,
            RENEWAL_FISCAL_QTR,
            ACCOUNT_NM,
            ACCOUNT_SECTOR_NM,
            ACCOUNT_INDUSTRY_NM,
            TARGET_CLOUD,
            CSG_TERRITORY,
            TEAM_TERRITORY,
            CSG_AREA,
            CSG_GEO,
            GEO,
            AE_FULL_NM,
            AE_ROLE_NM,
            ACCT_CSM,
            RENEWAL_OPTY_OWNR_NM,
            CONV_PRICE_UPLIFT_FORECAST_AMOUNT,
            MANAGER_FORECAST_JUDGEMENT,
            EARLY_RENEWAL_FLAG,
            DRVD_BU,
            ACCT_AOV_BAND,
            CNTR_AOV_BAND,
            SUCCESS_SEGMENT,
            SPECIALIST_SL_NT
        FROM {renewal_view}
        WHERE RENEWAL_OPTY_ID = %s
        {as_of_fragment}
        LIMIT 1
    """
    rows = run_query(
        sql,
        [opty_id_15] + as_of_bind,
        statement_timeout=_env_int("SNOWFLAKE_RENEWAL_STATEMENT_TIMEOUT", 120),
    )
    if rows:
        r = rows[0]
        fcast = (
            r.get("RENEWAL_FCAST_ATTRITION_CONV")
            if r.get("RENEWAL_FCAST_ATTRITION_CONV") is not None
            else r.get("RENEWAL_ATR_CONV")
        )
        fcast_atr = abs(float(fcast or 0))
        swing = abs(float(r.get("CONV_SWING_AMT") or 0))
        stg = r.get("RENEWAL_STG_NM") or ""

        territory = (
            r.get("CSG_TERRITORY")
            or r.get("TEAM_TERRITORY")
            or r.get("CSG_AREA")
            or ""
        )
        geo = r.get("CSG_GEO") or r.get("GEO") or ""

        out: dict[str, Any] = {
            "renewal_aov": float(
                r.get("RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV") or 0
            ),
            "renewal_atr_snow": fcast_atr,
            "renewal_swing_snow": swing,
            "renewal_fcast_code": r.get("RENEWAL_FCAST_CODE") or "",
            "account_name": r.get("ACCOUNT_NM"),
            "account_sector": r.get("ACCOUNT_SECTOR_NM") or "",
            "account_industry": r.get("ACCOUNT_INDUSTRY_NM") or "",
            "target_cloud": r.get("TARGET_CLOUD") or "",
            "csg_territory": territory,
            "csg_area": r.get("CSG_AREA") or "",
            "csg_geo": geo,
            "ae_name": r.get("AE_FULL_NM") or "",
            "ae_role": r.get("AE_ROLE_NM") or "",
            "csm_name": r.get("ACCT_CSM") or "",
            "renewal_manager": r.get("RENEWAL_OPTY_OWNR_NM") or "",
            "renewal_status": str(stg or ""),
            "risk_category": r.get("RENEWAL_KEY_RISK_CAT") or "",
            "risk_detail": r.get("RENEWAL_RISK_DETAIL") or "",
            "specialist_notes": r.get("SPECIALIST_SL_NT") or "",
            "renewal_close_date": str(r.get("RENEWAL_CLSD_DT") or ""),
            "renewal_close_month": str(r.get("RENEWAL_CLOSE_MONTH") or ""),
            "renewal_fiscal_qtr": r.get("RENEWAL_FISCAL_QTR") or "",
            "acct_aov_band": r.get("ACCT_AOV_BAND") or "",
            "cntr_aov_band": r.get("CNTR_AOV_BAND") or "",
            "success_segment": r.get("SUCCESS_SEGMENT") or "",
        }
        gmv = _gmv_rate_pct_from_renewal_row(r)
        if gmv:
            out["gmv_rate_pct"] = gmv
        return out
    return {}


def get_open_renewal_from_snowflake(
    search: str,
    cloud: str = "Commerce Cloud",
) -> Optional[dict]:
    """
    Snowflake-first: best open renewal row from ``WV_CI_RENEWAL_OPTY_SNAP_VW`` (excludes
    Closed/Dead stages; latest ``AS_OF_DATE``; highest prior ACV).

    Returns keys aligned with ``get_renewal_aov`` (static fields; Snowflake FCAST as
    ``renewal_atr_snow`` baseline only).
    """
    if str(os.getenv("SNOWFLAKE_RENEWAL_SKIP") or "").strip() in ("1", "true", "yes", "on"):
        return None

    renewal_view = _get_renewal_view()
    pinned = (os.getenv("SNOWFLAKE_RENEWAL_AS_OF_DATE") or "").strip()
    if pinned and renewal_view.endswith("WV_CI_RENEWAL_OPTY_SNAP_VW"):
        as_of_fragment = "AND AS_OF_DATE = %s"
        as_of_bind = [pinned]
    elif renewal_view.endswith("WV_CI_RENEWAL_OPTY_SNAP_VW"):
        as_of_fragment = """AND AS_OF_DATE = (
            SELECT MAX(AS_OF_DATE)
            FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_SNAP_VW
        )"""
        as_of_bind = []
    else:
        as_of_fragment = ""
        as_of_bind = []

    is_fsc = (
        "financial services" in str(cloud).lower()
        or str(cloud).strip().upper() == "FSC"
    )

    if is_fsc:
        cloud_filter = _renewal_cloud_filter_sql(cloud)
    else:
        cloud_filter = _renewal_cloud_filter_sql(cloud)

    search_clean = str(search).strip()
    search_safe = search_clean.replace("'", "''").replace("%", "%%")
    opty_bind = to_15_char_id(search_clean)

    sql = f"""
        SELECT
            RENEWAL_OPTY_ID,
            ACCOUNT_18_ID                            AS ACCOUNT_ID,
            ACCOUNT_NM                               AS ACCOUNT_NAME,
            RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV AS RENEWAL_AOV,
            RENEWAL_FCAST_ATTRITION_CONV,
            RENEWAL_ATR_CONV,
            CONV_SWING_AMT,
            RENEWAL_FCAST_CODE,
            RENEWAL_STG_NM,
            RENEWAL_KEY_RISK_CAT,
            RENEWAL_RISK_DETAIL,
            CSG_TERRITORY,
            TEAM_TERRITORY,
            CSG_AREA,
            CSG_GEO,
            GEO,
            AE_FULL_NM,
            ACCT_CSM,
            RENEWAL_OPTY_OWNR_NM,
            TARGET_CLOUD,
            ACCOUNT_SECTOR_NM,
            RENEWAL_CLOSE_MONTH,
            RENEWAL_CLSD_DT,
            ACCT_AOV_BAND,
            SUCCESS_SEGMENT
        FROM {renewal_view}
        WHERE {cloud_filter}
        AND (
            ACCOUNT_NM       LIKE '%%{search_safe}%%'
            OR RENEWAL_OPTY_NM LIKE '%%{search_safe}%%'
            OR RENEWAL_OPTY_ID = %s
        )
        AND RENEWAL_STG_NM NOT LIKE '%%Closed%%'
        AND RENEWAL_STG_NM NOT LIKE '%%Dead%%'
        {as_of_fragment}
        ORDER BY RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV DESC NULLS LAST
        LIMIT 1
    """
    try:
        rows = run_query(
            sql,
            [opty_bind] + as_of_bind,
            statement_timeout=_env_int("SNOWFLAKE_RENEWAL_STATEMENT_TIMEOUT", 120),
        )
        if rows:
            r = rows[0]
            fcast = (
                r.get("RENEWAL_FCAST_ATTRITION_CONV")
                if r.get("RENEWAL_FCAST_ATTRITION_CONV") is not None
                else r.get("RENEWAL_ATR_CONV")
            )
            swing = abs(float(r.get("CONV_SWING_AMT") or 0))
            stg = r.get("RENEWAL_STG_NM") or ""
            territory = (
                r.get("CSG_TERRITORY")
                or r.get("TEAM_TERRITORY")
                or r.get("CSG_AREA")
                or ""
            )
            geo = r.get("CSG_GEO") or r.get("GEO") or ""
            return {
                "opty_id": to_15_char_id(str(r.get("RENEWAL_OPTY_ID") or "")),
                "account_id": str(r.get("ACCOUNT_ID") or ""),
                "account_name": r.get("ACCOUNT_NAME") or "",
                "renewal_aov": float(r.get("RENEWAL_AOV") or 0),
                "renewal_atr_snow": abs(float(fcast or 0)),
                "renewal_swing_snow": swing,
                "renewal_fcast_code": r.get("RENEWAL_FCAST_CODE") or "",
                "ae_name": r.get("AE_FULL_NM") or "",
                "csm_name": r.get("ACCT_CSM") or "",
                "renewal_manager": r.get("RENEWAL_OPTY_OWNR_NM") or "",
                "renewal_status": str(stg or ""),
                "risk_category": r.get("RENEWAL_KEY_RISK_CAT") or "",
                "risk_detail": r.get("RENEWAL_RISK_DETAIL") or "",
                "csg_territory": territory,
                "csg_area": r.get("CSG_AREA") or "",
                "csg_geo": geo,
                "target_cloud": r.get("TARGET_CLOUD") or "",
                "account_sector": r.get("ACCOUNT_SECTOR_NM") or "",
                "renewal_close_month": str(r.get("RENEWAL_CLOSE_MONTH") or ""),
                "renewal_close_date": str(r.get("RENEWAL_CLSD_DT") or ""),
            }
    except Exception as e:
        log_debug(f"get_open_renewal_from_snowflake error: {str(e)[:80]}")
    return None


def _apm_product_display_name(row: dict) -> str:
    """Prefer APM_LVL_3, then L2, then L1 (L3 is often NULL in CSS)."""
    for key in ("APM_LVL_3", "APM_LVL_2", "APM_LVL_1"):
        v = row.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return "Unknown"


def _normalize_attrition_row(r: dict) -> dict:
    """Single CSS attrition row → structure used by GM Review / Slack / enrich."""
    return {
        "product": _apm_product_display_name(r),
        "APM_LVL_1": r.get("APM_LVL_1"),
        "APM_LVL_2": r.get("APM_LVL_2"),
        "APM_LVL_3": r.get("APM_LVL_3"),
        "ATTRITION_PIPELINE": r.get("ATTRITION_PIPELINE"),
        "ATTRITION_PROBA": r.get("ATTRITION_PROBA"),
        "ATTRITION_PROBA_CATEGORY": r.get("ATTRITION_PROBA_CATEGORY"),
        "ATTRITION_REASON": r.get("ATTRITION_REASON"),
        "attrition": abs(float(r.get("ATTRITION_PIPELINE") or 0)),
        "category": r.get("ATTRITION_PROBA_CATEGORY"),
        "reason": r.get("ATTRITION_REASON") or "",
        "factors_incr": r.get("FACTORS_INCR_RISK") or "",
        "factors_decr": r.get("FACTORS_DECR_RISK") or "",
    }


def _apm_cloud_match_variants(cloud: str) -> list[str]:
    """Lowercased substring needles aligned with ``apm_cloud_levels_predicate`` (LIKE %%v%%)."""
    if not cloud or str(cloud).strip() == "" or str(cloud) == "All Clouds":
        return []
    c = str(cloud).strip()
    variants = [c.lower()]
    first = c.split(None, 1)[0] if c else ""
    if first and first != c and len(first) >= 3:
        variants.append(first.lower())
    seen: set[str] = set()
    out: list[str] = []
    for v in variants:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def filter_products_by_cloud(products: list, cloud: str | None) -> list:
    """
    Filter normalized attrition products by cloud in Python (substring on APM_LVL_1/2/3).
    If no row matches, returns ``products`` unchanged (same as SQL retry without cloud).
    """
    if not products:
        return []
    if not cloud or str(cloud).strip() == "" or str(cloud) == "All Clouds":
        return list(products)

    c_strip = str(cloud).strip()
    c_low = c_strip.lower()
    variants = _apm_cloud_match_variants(c_strip)
    if (
        c_low in ("financial services cloud", "fsc")
        or "financial services" in c_low
    ):
        variants = list(dict.fromkeys(list(variants) + ["industries"]))
    if not variants:
        return list(products)

    def row_matches(p: dict) -> bool:
        for k in ("APM_LVL_1", "APM_LVL_2", "APM_LVL_3"):
            cell = str(p.get(k, "") or "").lower()
            if not cell:
                continue
            if any(v in cell for v in variants):
                return True
        return False

    filtered = [p for p in products if row_matches(p)]
    return filtered if filtered else list(products)


def get_account_attrition_all(account_id: str) -> dict[str, Any]:
    """
    Single wide pull of attrition products for an account (latest CSS snapshot, no cloud predicate).

    Returns normalized ``all`` rows plus ``raw`` Snowflake dicts. Callers filter by cloud with
    ``filter_products_by_cloud`` to avoid a second round-trip.
    """
    if str(os.getenv("SNOWFLAKE_CSS_SKIP") or "").strip() in ("1", "true", "yes", "on"):
        return {"all": [], "raw": []}
    conditions = [
        "ACCOUNT_ID = %s",
        "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM "
        "SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT)",
    ]
    params: list[Any] = [to_15_char_id(account_id)]
    where_clause = " AND ".join(conditions)
    sql = f"""
        SELECT
            APM_LVL_1, APM_LVL_2, APM_LVL_3,
            ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
            ATTRITION_REASON, ATTRITION_PIPELINE
        FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE {where_clause}
        ORDER BY ATTRITION_PIPELINE DESC NULLS LAST
        LIMIT 50
    """
    raw = run_query(
        sql,
        params,
        statement_timeout=_env_int("SNOWFLAKE_ATTRITION_STATEMENT_TIMEOUT", 90),
    )
    all_products = [_normalize_attrition_row(r) for r in raw]
    return {"all": all_products, "raw": raw}


def get_account_attrition_all_cached(account_id: str) -> dict[str, Any]:
    key = f"attrition_all|{to_15_char_id(account_id)}"
    cached = _cache_get(_account_enrich_cache, key)
    if cached is not None:
        log_debug(f"attrition_all cache HIT: {account_id}")
        return cached
    result = get_account_attrition_all(account_id)
    _cache_set(_account_enrich_cache, key, result)
    return result


def get_account_attrition(account_id: str, cloud: str | None = "Commerce Cloud") -> list:
    """
    Product-level attrition on latest CSS snapshot.
    ``cloud=None`` (or empty / ``All Clouds``): all products, no APM cloud predicate.
    """
    if str(os.getenv("SNOWFLAKE_CSS_SKIP") or "").strip() in ("1", "true", "yes", "on"):
        return []
    conditions = [
        "ACCOUNT_ID = %s",
        "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM "
        "SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT)",
    ]
    params: list[Any] = [to_15_char_id(account_id)]

    use_cloud = (
        cloud is not None
        and str(cloud).strip()
        and str(cloud) != "All Clouds"
    )

    def _run(extra_predicate: str | None) -> list:
        cond = list(conditions)
        if extra_predicate:
            cond.append(extra_predicate)
        where_clause = " AND ".join(cond)
        sql = f"""
            SELECT
                APM_LVL_1, APM_LVL_2, APM_LVL_3,
                ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
                ATTRITION_REASON, ATTRITION_PIPELINE
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            WHERE {where_clause}
            ORDER BY ATTRITION_PIPELINE DESC NULLS LAST
            LIMIT 50
        """
        return run_query(sql, params)

    pred = apm_cloud_levels_predicate(str(cloud).strip()) if use_cloud else ""
    rows = _run(pred if pred else None)
    if not rows and use_cloud and pred:
        log_debug("get_account_attrition: no rows with cloud filter; retrying without")
        rows = _run(None)

    return [_normalize_attrition_row(r) for r in rows]


def format_enrichment_for_display(enrichment: dict) -> dict:
    if not enrichment:
        return {}

    ari = enrichment.get("ari", {})
    ari_cat = ari.get("category", "Unknown")
    if ari_cat == "High":
        ari_emoji = ":red_circle:"
    elif ari_cat == "Medium":
        ari_emoji = ":large_yellow_circle:"
    elif ari_cat == "Low":
        ari_emoji = ":large_green_circle:"
    else:
        ari_emoji = ":white_circle:"

    ari_prob = ari.get("probability")
    if ari_prob is not None:
        try:
            prob_float = float(ari_prob)
            if prob_float <= 1.0:
                prob_display = f"{prob_float * 100:.1f}%"
            else:
                prob_display = f"{prob_float:.1f}%"
        except (TypeError, ValueError):
            prob_display = "N/A"
    else:
        prob_display = "N/A"

    result = {
        "ari_category": ari_cat,
        "ari_probability": prob_display,
        "ari_emoji": ari_emoji,
        "ari_reason": ari.get("reason", "N/A"),
        "territory": "N/A",
        "csg_territory": "",
        "csg_geo": "N/A",
        "burn_rate": "N/A",
        "gmv_rate": "N/A",
    }

    health = enrichment.get("health", {})
    health_score = health.get("overall_score")
    health_literal = health.get("overall_literal", "Unknown")
    if health_literal in (None, ""):
        health_literal = "Unknown"

    # overall_literal already normalized in get_customer_health()
    if health_score:
        try:
            hs = float(health_score)
            if hs >= 70:
                health_display = f":large_green_circle: Green ({int(hs)})"
            elif hs >= 40:
                health_display = f":large_yellow_circle: Yellow ({int(hs)})"
            else:
                health_display = f":red_circle: Red ({int(hs)})"
        except (TypeError, ValueError):
            health_display = ":white_circle: Unknown"
    else:
        health_display = ":white_circle: Unknown"

    result["health_score"] = health_score
    result["health_literal"] = health_literal
    result["health_display"] = health_display

    usage = enrichment.get("usage", {})
    cloud_aov = usage.get("cloud_aov", "Unknown")
    util_rate = usage.get("utilization_rate", "Unknown")
    util_emoji = ":white_circle:"
    if util_rate not in ("Unknown", "N/A", None, ""):
        try:
            util_val = float(str(util_rate).rstrip("%").strip())
            if util_val >= 70:
                util_emoji = ":large_green_circle:"
            elif util_val >= 40:
                util_emoji = ":large_yellow_circle:"
            else:
                util_emoji = ":red_circle:"
        except (TypeError, ValueError):
            pass

    result["cc_aov"] = cloud_aov
    result["utilization_rate"] = util_rate
    result["util_emoji"] = util_emoji

    renewal = enrichment.get("renewal_aov") or {}
    if renewal:
        result["renewal_aov"] = renewal
        atr_snow = renewal.get("renewal_atr_snow")
        if atr_snow is None and renewal.get("renewal_atr") is not None:
            atr_snow = renewal.get("renewal_atr")
        if atr_snow is not None:
            result["renewal_atr"] = atr_snow
        if renewal.get("gmv_rate_pct") is not None:
            result["gmv_rate"] = renewal.get("gmv_rate_pct")
        result["csg_geo"] = renewal.get("csg_geo", "N/A")
        ct = renewal.get("csg_territory")
        result["csg_territory"] = (str(ct).strip() if ct else "") or ""
        result["territory"] = (
            renewal.get("csg_territory")
            or renewal.get("csg_area")
            or renewal.get("csg_geo")
            or "N/A"
        )
        tc = renewal.get("target_cloud")
        if tc is not None and str(tc).strip():
            result["target_cloud"] = str(tc).strip()

    # Swing — from Snowflake renewal row (no Org62 needed)
    renewal = enrichment.get("renewal_aov") or {}
    swing_snow = renewal.get("renewal_swing_snow")
    try:
        swing_ok = swing_snow is not None and float(swing_snow) > 0
    except (TypeError, ValueError):
        swing_ok = False
    if swing_ok:
        result["swing"] = fmt_amount(float(swing_snow))
    else:
        result["swing"] = "N/A"

    return result


def format_enrichment_for_claude(enrichment: dict) -> str:
    if not enrichment:
        return ""
    display = format_enrichment_for_display(enrichment)
    _ap = display.get("ari_probability", "N/A")
    _ap_paren = (
        _ap
        if (_ap == "N/A" or str(_ap).strip().endswith("%"))
        else f"{_ap}%"
    )
    return "\n".join([
        f"ARI: {display.get('ari_category', 'N/A')} ({_ap_paren})",
        f"Utilization: {display.get('utilization_rate', 'N/A')}",
        f"GMV Rate: {display.get('gmv_rate', 'N/A')}",
        f"Territory: {display.get('territory', 'N/A')}",
        f"Health: {display.get('health_display', 'N/A')}",
    ])


def _resolve_account_from_snowflake_css(account_name: str) -> Optional[dict]:
    """Last-resort name match on latest CSS attrition snapshot."""
    if str(os.getenv("SNOWFLAKE_CSS_SKIP") or "").strip() in ("1", "true", "yes", "on"):
        return None

    if not account_name:
        return None
    try:
        # Attrition product grain has no reliable account-name column; join renewal view
        # (same pattern as slack_app at-risk query: atr.ACCOUNT_ID = ren.ACCT_ID).
        rows = run_query(
            """
            SELECT ren.ACCOUNT_18_ID AS ACCOUNT_ID, ren.ACCOUNT_NM AS ACCOUNT_NAME
            FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT atr
            INNER JOIN SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_SNAP_VW ren
                ON atr.ACCOUNT_ID = ren.ACCT_ID
            WHERE UPPER(ren.ACCOUNT_NM) LIKE UPPER(%s)
            AND atr.SNAPSHOT_DT = (
                SELECT MAX(SNAPSHOT_DT)
                FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
            )
            LIMIT 1
            """,
            [f"%{account_name.strip()}%"],
        )
        if rows:
            return {
                "account_id": rows[0].get("ACCOUNT_ID"),
                "account_name": rows[0].get("ACCOUNT_NAME") or rows[0].get("ACCOUNT_NM"),
            }
    except Exception as e:
        log_debug(f"CSS Snowflake account resolve error: {str(e)[:80]}")
    return None


def resolve_account_from_snowflake(
    name: str, cloud: str = "Commerce Cloud"
) -> Optional[dict]:
    """
    Resolve account from Snowflake renewal view using parallel fuzzy LIKE patterns.

    Returns ``account_id``, ``account_name``, plus open-renewal row fields when matched:
    ``opty_id`` (15-char), ``renewal_aov``, ``renewal_atr_snow``, ``csg_territory``,
    ``csg_geo``, ``target_cloud``.

    Each pattern uses the latest ``AS_OF_DATE`` snapshot, excludes closed/dead stages,
    and orders by ``RENEWAL_AMT_CONV`` for deterministic tie-breaks on duplicate names.
    """
    if not name:
        return None

    renewal_view = _get_renewal_view()
    pinned = (os.getenv("SNOWFLAKE_RENEWAL_AS_OF_DATE") or "").strip()
    if pinned and renewal_view.endswith("WV_CI_RENEWAL_OPTY_SNAP_VW"):
        as_of_fragment = "AND ren.AS_OF_DATE = %s"
    elif renewal_view.endswith("WV_CI_RENEWAL_OPTY_SNAP_VW"):
        as_of_fragment = """AND ren.AS_OF_DATE = (
                    SELECT MAX(AS_OF_DATE)
                    FROM SSE_DM_CSG_RPT_PRD.RENEWALS.WV_CI_RENEWAL_OPTY_SNAP_VW
                )"""
    else:
        as_of_fragment = ""

    search_clean = name.strip()
    search_stripped = re.sub(
        CORPORATE_SUFFIXES, "", search_clean, flags=re.IGNORECASE
    ).strip().rstrip(",").strip()
    search_words = search_stripped.split()

    patterns: list[str] = [search_clean, f"%{search_clean}%"]
    if search_stripped:
        patterns.append(f"%{search_stripped}%")
    if search_words and len(search_words[0]) > 3:
        patterns.append(f"%{search_words[0]}%")

    def try_pattern(pattern: str, priority: int) -> Optional[dict]:
        try:
            is_fsc = (
                "financial services" in str(cloud).lower()
                or str(cloud).strip().upper() == "FSC"
            )
            # LIKE literals use %% — Snowflake connector runs sql % params (pyformat).
            if is_fsc:
                cloud_filter = _renewal_cloud_filter_sql(cloud, alias="ren")
            else:
                cloud_filter = _renewal_cloud_filter_sql(cloud, alias="ren")

            sql = f"""
                SELECT
                    ren.ACCOUNT_18_ID                            AS ACCOUNT_ID,
                    ren.ACCOUNT_NM                               AS ACCOUNT_NAME,
                    ren.RENEWAL_OPTY_ID,
                    ren.RENEWAL_PRIOR_ANNUAL_CONTRACT_VALUE_CONV AS RENEWAL_AOV,
                    ren.RENEWAL_FCAST_ATTRITION_CONV             AS RENEWAL_ATR_SNOW,
                    ren.CSG_TERRITORY,
                    ren.CSG_AREA,
                    ren.CSG_GEO,
                    ren.TARGET_CLOUD
                FROM {renewal_view} ren
                WHERE ren.ACCOUNT_NM LIKE %s
                AND {cloud_filter}
                AND ren.RENEWAL_STG_NM NOT IN (
                    'Dead Attrition', '05 Closed', 'Dead - Duplicate',
                    'Dead - No Decision', 'Dead - No Opportunity',
                    'NP - Dead Duplicate', '08 - Closed', 'Closed',
                    'Closed and referral paid', 'Loss - Off Contract',
                    'UNKNOWN', 'Courtesy'
                )
                {as_of_fragment}
                ORDER BY ren.RENEWAL_AMT_CONV DESC NULLS LAST
                LIMIT 1
            """
            binds = [pattern] + ([pinned] if pinned else [])
            rows = run_query(
                sql,
                binds,
                statement_timeout=_env_int("SNOWFLAKE_RENEWAL_STATEMENT_TIMEOUT", 120),
            )
            if rows:
                r = rows[0]
                return {
                    "account_id": r.get("ACCOUNT_ID"),
                    "account_name": r.get("ACCOUNT_NAME"),
                    "opty_id": to_15_char_id(str(r.get("RENEWAL_OPTY_ID") or "")),
                    "renewal_aov": float(r.get("RENEWAL_AOV") or 0),
                    "renewal_atr_snow": abs(float(r.get("RENEWAL_ATR_SNOW") or 0)),
                    "csg_territory": r.get("CSG_TERRITORY") or "",
                    "csg_area": r.get("CSG_AREA") or "",
                    "csg_geo": r.get("CSG_GEO") or "",
                    "target_cloud": r.get("TARGET_CLOUD") or "",
                    "priority": priority,
                }
        except Exception as e:
            log_debug(f"Snowflake pattern resolve error: {str(e)[:60]}")
        return None

    best: Optional[dict] = None
    best_priority = 999
    with ThreadPoolExecutor(max_workers=max(1, len(patterns))) as executor:
        future_map = {
            executor.submit(try_pattern, p, idx): idx
            for idx, p in enumerate(patterns)
        }
        for fut in as_completed(future_map, timeout=25):
            try:
                res = fut.result(timeout=10)
                if res and res["priority"] < best_priority:
                    best = res
                    best_priority = res["priority"]
                    if best_priority == 0:
                        break
            except Exception:
                continue

    if best:
        return {
            "account_id": best["account_id"],
            "account_name": best["account_name"],
            "opty_id": best.get("opty_id") or "",
            "renewal_aov": float(best.get("renewal_aov") or 0),
            "renewal_atr_snow": float(best.get("renewal_atr_snow") or 0),
            "csg_territory": best.get("csg_territory") or "",
            "csg_area": best.get("csg_area") or "",
            "csg_geo": best.get("csg_geo") or "",
            "target_cloud": best.get("target_cloud") or "",
        }

    return _resolve_account_from_snowflake_css(search_clean)


def resolve_account_from_snowflake_cached(name, cloud="Commerce Cloud"):
    key = f"{name.lower().strip()}|{cloud}"
    cached = _cache_get(_account_resolve_cache, key)
    if cached is not None:
        log_debug(f"resolve_account cache HIT: {name}")
        return cached
    result = resolve_account_from_snowflake(name, cloud)
    if result:
        _cache_set(_account_resolve_cache, key, result)
    return result


def get_at_risk_accounts_snowflake(
    cloud: str | None = None,
    risk_category: str | None = None,
    min_attrition: float = 0,
    limit: int = 25,
    min_aov: float = 0,
    ari_filter: str | None = None,
    sort_by: str = "atr",
):
    del min_aov
    conditions = [
        "ACCOUNT_ID IS NOT NULL",
        "SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) FROM "
        "SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT)",
    ]

    if cloud and cloud != "All Clouds":
        pred = apm_cloud_levels_predicate(cloud)
        if pred:
            conditions.append(pred)

    if ari_filter:
        safe = str(ari_filter).replace("'", "''")
        conditions.append(f"ATTRITION_PROBA_CATEGORY = '{safe}'")
    elif risk_category:
        safe_rc = str(risk_category).replace("'", "''")
        conditions.append(f"ATTRITION_PROBA_CATEGORY = '{safe_rc}'")

    if min_attrition > 0:
        conditions.append(f"ABS(ATTRITION_PIPELINE) > {min_attrition}")

    conditions.append(
        "LOWER(COALESCE(APM_LVL_2, '')) NOT LIKE '%success plan%'"
    )
    conditions.append(
        "LOWER(COALESCE(APM_LVL_3, '')) NOT LIKE '%success plan%'"
    )

    where_clause = " AND ".join(conditions)
    sort_map = {
        "atr": "ABS(ATTRITION_PIPELINE) DESC",
        "ari": "ATTRITION_PROBA DESC",
        "aov": "ATTRITION_PIPELINE DESC",
        "cc_aov": "ATTRITION_PIPELINE DESC",
    }
    order_by = sort_map.get(sort_by, "ABS(ATTRITION_PIPELINE) DESC")

    sql = f"""
        SELECT
            ACCOUNT_ID,
            APM_LVL_1, APM_LVL_2, APM_LVL_3,
            ATTRITION_PROBA, ATTRITION_PROBA_CATEGORY,
            ATTRITION_REASON, ATTRITION_PIPELINE,
            SNAPSHOT_DT
        FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
        WHERE {where_clause}
        ORDER BY {order_by}
        LIMIT {int(limit)}
    """
    try:
        raw = run_query(sql, [])
    except Exception as e:
        log_error(f"get_at_risk_accounts_snowflake error: {e}")
        return []

    out = []
    for r in raw:
        out.append({
            "account_id": r.get("ACCOUNT_ID"),
            "account_name": r.get("ACCOUNT_NAME") or "",
            "apm_lvl_1": r.get("APM_LVL_1"),
            "apm_lvl_2": r.get("APM_LVL_2"),
            "apm_lvl_3": r.get("APM_LVL_3"),
            # Uppercase aliases for callers/tests expecting SQL-style keys
            "ACCOUNT_ID": r.get("ACCOUNT_ID"),
            "APM_LVL_2": r.get("APM_LVL_2"),
            "APM_LVL_3": r.get("APM_LVL_3"),
            "ATTRITION_PROBA_CATEGORY": r.get("ATTRITION_PROBA_CATEGORY"),
            "ATTRITION_PIPELINE": r.get("ATTRITION_PIPELINE"),
            "attrition_pipeline": float(r.get("ATTRITION_PIPELINE") or 0),
            "attrition_proba_category": r.get("ATTRITION_PROBA_CATEGORY"),
            "attrition_reason": r.get("ATTRITION_REASON"),
            "snapshot_dt": str(r.get("SNAPSHOT_DT") or ""),
        })
    return out


def _escape_sf_id(account_id: str) -> str:
    return str(account_id).replace("'", "")


class SnowflakeClient:
    """Singleton OOP wrapper over the module connection pool (borrow/return per query)."""

    _instance: Optional["SnowflakeClient"] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "SnowflakeClient":
        # args/kwargs are for __init__ only; Python still passes them to __new__
        if cls._instance is None:
            inst = super().__new__(cls)
            inst._initialized = False
            cls._instance = inst
        return cls._instance

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        authenticator: Optional[str] = None,
    ) -> None:
        if self._initialized:
            return
        self._account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self._user = user or os.getenv("SNOWFLAKE_USER")
        if not self._account or not self._user:
            raise ValueError("SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER are required")
        _init_pool()
        self._initialized = True

    def get_account_usage(
        self, account_id: str, cloud: str = "Commerce Cloud"
    ) -> Optional[dict[str, Any]]:
        try:
            u = get_usage_unified(to_15_char_id(account_id), cloud)
            usage = u.get("summary") or {}
            if usage:
                return {
                    "utilization_rate": usage.get("utilization_rate", "N/A"),
                    "util_emoji": usage.get("util_emoji", ":white_circle:"),
                    "gmv_util": usage.get("gmv_util"),
                    "source": usage.get("source", ""),
                }
        except Exception as e:
            log_error(f"SnowflakeClient.get_account_usage error: {str(e)[:100]}")
        return None

    def get_ari_score(self, account_id: str) -> Optional[float]:
        aid = _escape_sf_id(account_id)
        conn = get_snowflake_connection()
        try:
            cursor = conn.cursor()
            try:
                query = f"""
                SELECT ATTRITION_PROBA * 100 AS probability
                FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
                WHERE ACCOUNT_ID = '{aid}'
                AND SNAPSHOT_DT = (
                    SELECT MAX(SNAPSHOT_DT)
                    FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
                )
                ORDER BY ATTRITION_PROBA DESC
                LIMIT 1
                """
                cursor.execute(query)
                row = cursor.fetchone()
                if not row or row[0] is None:
                    return None
                return round(float(row[0]), 1)
            finally:
                cursor.close()
        except Exception as e:
            log_error(f"SnowflakeClient.get_ari_score error: {e}")
            return None
        finally:
            return_connection(conn)

    def get_attrition_signals(self, account_id: str) -> Optional[dict[str, Any]]:
        aid = _escape_sf_id(account_id)
        conn = get_snowflake_connection()
        try:
            cursor = conn.cursor()
            try:
                query = f"""
                SELECT
                    APM_LVL_3 AS product,
                    ABS(ATTRITION_PIPELINE) AS attrition,
                    ATTRITION_PROBA_CATEGORY AS category
                FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
                WHERE ACCOUNT_ID = '{aid}'
                AND SNAPSHOT_DT = (
                    SELECT MAX(SNAPSHOT_DT)
                    FROM SSE_DM_CSG_RPT_PRD.CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
                )
                ORDER BY ABS(ATTRITION_PIPELINE) DESC
                """
                cursor.execute(query)
                rows = cursor.fetchall()
                products = [
                    {
                        "product": r[0],
                        "attrition": float(r[1]) if r[1] is not None else 0.0,
                        "category": r[2],
                    }
                    for r in rows
                ]
                return {
                    "account_id": account_id,
                    "products": products,
                    "count": len(products),
                }
            finally:
                cursor.close()
        except Exception as e:
            log_error(f"SnowflakeClient.get_attrition_signals error: {e}")
            return None
        finally:
            return_connection(conn)

    def close(self) -> None:
        """No-op: connections are pooled; callers use return_connection via run_query."""
        pass


# --- SF Products label cleanup (canvas, Sheets, exporters) -----------------

APM_L1_DISPLAY_MAP: dict[str, str] = {
    "Salesforce Platform": "Platform",
    "Integration": "MuleSoft",
    "AI and Data": "Data Cloud",
    "Cross Cloud - CRM": "CRM",
    "Cross Cloud - Einstein": "Einstein",
    "Industries": "Industries",
    "Sales Cloud & Industries": "FSC Sales",
    "Service Cloud & Industries": "FSC Service",
}

APM_L1_EXCLUDE = frozenset({"Other", ""})


def get_sf_products_display(all_products: list) -> str:
    """Deduped APM L1 labels for Salesforce products (maps long L1 names to short labels)."""
    if not all_products:
        return "N/A"

    unique_l1s = list(
        dict.fromkeys(
            str(p.get("APM_LVL_1") or "").strip()
            for p in all_products
            if str(p.get("APM_LVL_1") or "").strip()
        )
    )

    cleaned: list[str] = []
    for l1 in unique_l1s:
        if l1 in APM_L1_EXCLUDE:
            continue
        display = APM_L1_DISPLAY_MAP.get(l1, l1)
        if display not in cleaned:
            cleaned.append(display)

    return ", ".join(cleaned) if cleaned else "N/A"
