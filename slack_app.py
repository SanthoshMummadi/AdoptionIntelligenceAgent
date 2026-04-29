from slack_bolt import App, BoltContext
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
import os
import re
import sys
import time
import threading
import json
import pickle
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone

from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _ROOT)
load_dotenv(os.path.join(_ROOT, ".env"))
import server  # side effect: startup env validation (see server._should_run_startup_env_validation)
from domain.analytics.snowflake_client import (
    clear_stale_caches,
    clear_usage_snapshot_cache,
    prewarm_cidm_usage_snapshot_dt,
    prewarm_renewal_as_of_date,
)
from domain.tracking.account_tracker import setup_tracking_tables
from services.adoption_heatmap_workflow import (
    get_available_clouds,
    classify_adoption_intent,
)
from services.daily_pulse_workflow import run_daily_pulse
from domain.content.heatmap_builder import (
    build_adoption_heatmap_blocks,
    build_group_drilldown_blocks,
    build_feature_detail_blocks,
    build_home_loading_blocks,
)
from domain.analytics.heatmap_queries import (
    get_adoption_heatmap_data,
    get_feature_account_movers,
    _CLOUD_MAPPING,
    resolve_cloud,
    resolve_cloud_key,
    VALID_REGIONS,
    VALID_INDUSTRIES,
)
from domain.analytics.threshold_config import reload_thresholds
from log_utils import log_error

logger = logging.getLogger(__name__)

slack_bot_token = os.environ.get("SLACK_BOT_TOKEN")
if not slack_bot_token:
    raise RuntimeError(
        "Missing SLACK_BOT_TOKEN. Set it in your shell or add it to .env as:\n"
        "SLACK_BOT_TOKEN=xoxb-...\n"
    )

app = App(token=slack_bot_token)
_pulse_scheduler = None
WATCHLIST_FREQUENCY = os.getenv("WATCHLIST_ALERT_FREQUENCY", "daily").lower()
WATCHLIST_THRESHOLD = float(os.getenv("WATCHLIST_DROP_THRESHOLD", "15"))
WATCHLIST_MIN_SCORE = float(os.getenv("WATCHLIST_MIN_SCORE", "5"))
WATCHLIST_DEMO_MODE = os.getenv("WATCHLIST_DEMO_MODE", "false").lower() == "true"
# In-memory context store for heatmap thread replies
# Shape: { channel_id: { cloud, fy, features, ts, created } }
HEATMAP_CONTEXT: dict = {}
HOME_STATE: dict = {}  # { user_id: { "status": str, ... } }
_LAST_CLOUD_SELECT: dict = {}  # { user_id: timestamp }


def _home_state_get(user_id: str) -> dict:
    st = HOME_STATE.get(user_id)
    if isinstance(st, dict):
        return st
    if isinstance(st, str):
        return {"status": st}
    return {}


def _home_state_set(user_id: str, **kwargs) -> None:
    cur = _home_state_get(user_id)
    cur.update(kwargs)
    HOME_STATE[user_id] = cur


HOME_ADOPTION_CLOUD_OPTIONS = [
    {"text": {"type": "plain_text", "text": "🛒 Commerce B2B"}, "value": "Commerce B2B"},
    {"text": {"type": "plain_text", "text": "☁️ Sales Cloud"}, "value": "Sales Cloud"},
    {"text": {"type": "plain_text", "text": "🏦 FSC"}, "value": "FSC"},
    {"text": {"type": "plain_text", "text": "🤖 Agentforce Runtime & Trust"}, "value": "Agentforce Runtime & Trust"},
    {"text": {"type": "plain_text", "text": "🧪 Agentforce Build, Test, Observe"}, "value": "Agentforce Build, Test, Observe"},
    {"text": {"type": "plain_text", "text": "🧠 Agentforce Agent Types"}, "value": "Agentforce Agent Types"},
    {"text": {"type": "plain_text", "text": "🎫 Agentforce IT Service"}, "value": "Agentforce IT Service"},
]


def build_home_initial_blocks() -> list:
    return [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "📊 PM Intelligence Hub"},
        },
        {"type": "divider"},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "*Select a module to get started:*"},
            "accessory": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Choose module...",
                },
                "action_id": "home_module_select",
                "options": [
                    {
                        "text": {"type": "plain_text", "text": "📊 Adoption"},
                        "value": "adoption",
                    },
                    {
                        "text": {"type": "plain_text", "text": "📉 Attrition"},
                        "value": "attrition",
                    },
                ],
            },
        },
    ]


def build_attrition_home_blocks() -> list:
    return [
        {"type": "divider"},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*📉 Attrition Dashboard*\n"
                    "Use slash commands to run attrition workflows:"
                ),
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "• `/attrition-risk <Account Name>`"},
                {"type": "mrkdwn", "text": "• `/attrition-clouds`"},
                {"type": "mrkdwn", "text": "• `/at-risk-canvas <filters>`"},
                {"type": "mrkdwn", "text": "• `/gm-review-canvas <accounts/opps>`"},
            ],
        },
    ]


def on_startup():
    """Initialize required DB tables on bot startup."""
    try:
        setup_tracking_tables()
        print("✓ Tracking tables initialized")
    except Exception as e:
        log_error(f"Failed to initialize tracking tables: {e}")


on_startup()
clear_usage_snapshot_cache()
prewarm_cidm_usage_snapshot_dt()
prewarm_renewal_as_of_date()
print("✓ CIDM + Renewal snapshots prewarmed")


def setup_pulse_scheduler():
    """Setup pulse schedule from env (daily/weekly/hourly)."""
    global _pulse_scheduler
    frequency = os.getenv("PULSE_FREQUENCY", "daily").lower()
    schedule_time = os.getenv("PULSE_SCHEDULE_TIME", "09:00")
    pulse_channel = os.getenv("PULSE_CHANNEL", "").strip() or None

    try:
        hour_s, minute_s = schedule_time.split(":")
        hour = int(hour_s)
        minute = int(minute_s)
    except Exception:
        hour, minute = 9, 0
        print("⚠️ Invalid PULSE_SCHEDULE_TIME; defaulting to 09:00")

    ist = "Asia/Kolkata"
    scheduler = BackgroundScheduler(timezone=ist)
    if frequency == "daily":
        trigger = CronTrigger(hour=hour, minute=minute)
        print(f"✓ Scheduling daily pulse at {hour:02d}:{minute:02d} IST")
    elif frequency == "weekly":
        trigger = CronTrigger(day_of_week="mon", hour=hour, minute=minute)
        print(f"✓ Scheduling weekly pulse (Mon) at {hour:02d}:{minute:02d} IST")
    elif frequency == "hourly":
        trigger = CronTrigger(minute=minute)
        print(f"✓ Scheduling hourly pulse at :{minute:02d} (testing)")
    else:
        trigger = CronTrigger(hour=hour, minute=minute)
        print(
            f"⚠️ Unknown PULSE_FREQUENCY={frequency}; "
            f"defaulting daily at {hour:02d}:{minute:02d} IST"
        )

    scheduler.add_job(
        lambda: run_daily_pulse(app.client, pulse_channel),
        trigger=trigger,
        id="daily_pulse",
        name="Daily Pulse",
        replace_existing=True,
    )
    scheduler.add_job(
        lambda: send_daily_pulse(app.client),
        trigger=CronTrigger(hour=hour, minute=minute, timezone=ist),
        id="daily_watchlist_pulse",
        name="Daily Watchlist Pulse",
        replace_existing=True,
    )
    scheduler.add_job(
        clear_stale_caches,
        CronTrigger(minute=0),
        id="clear_stale_caches",
        replace_existing=True,
    )
    if WATCHLIST_DEMO_MODE:
        watch_trigger = CronTrigger(minute="*")
        print("✓ Watchlist demo mode enabled (runs every minute)")
    else:
        watch_trigger = CronTrigger(hour=hour, minute=minute)
    scheduler.add_job(
        lambda: check_watchlist_alerts(app.client),
        trigger=watch_trigger,
        id="watchlist_alerts",
        name="Watchlist Alerts",
        replace_existing=True,
    )
    scheduler.start()
    _pulse_scheduler = scheduler
    print("✓ Pulse scheduler started")


setup_pulse_scheduler()


def _resolve_atr_for_tldr(snowflake_display: dict | None, opp: dict | None) -> str:
    """
    Prefer Salesforce ``Forecasted_Attrition__c``; fall back to Snowflake FCAST baseline
    (``renewal_atr_snow``).
    """
    from domain.analytics.snowflake_client import extract_usd, fmt_amount

    sf_atr = (opp or {}).get("Forecasted_Attrition__c")
    if sf_atr is not None and str(sf_atr).strip() != "":
        return fmt_amount(extract_usd(sf_atr))

    disp = snowflake_display or {}
    atr_val = disp.get("renewal_atr")
    if atr_val is None:
        renewal = disp.get("renewal_aov") or {}
        atr_val = renewal.get("renewal_atr_snow") or renewal.get("renewal_atr")
    if atr_val is not None and str(atr_val).strip() != "":
        try:
            return fmt_amount(float(atr_val))
        except (TypeError, ValueError):
            return fmt_amount(atr_val)
    return "N/A"


# -------------------------
# Persistence
# -------------------------
STORAGE_DIR = os.path.join(os.path.dirname(__file__), "storage")
os.makedirs(STORAGE_DIR, exist_ok=True)

LAST_BRIEF_FILE = os.path.join(STORAGE_DIR, "user_last_brief.pkl")
DB_PATH = os.path.join(os.path.dirname(__file__), "bot_history.db")


def load_last_briefs() -> dict:
    if os.path.exists(LAST_BRIEF_FILE):
        try:
            with open(LAST_BRIEF_FILE, "rb") as f:
                return pickle.load(f)
        except Exception:
            return {}
    return {}


def save_last_briefs() -> None:
    try:
        with open(LAST_BRIEF_FILE, "wb") as f:
            pickle.dump(user_last_brief, f)
    except Exception as e:
        print(f"❌ Error saving last briefs: {e}")


user_last_brief: dict[str, str] = load_last_briefs()
print(f"✅ Loaded last brief info for {len(user_last_brief)} users")


# -------------------------
# Session archive (SQLite)
# -------------------------
active_sessions: dict[str, dict] = {}


def init_database() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS conversation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                session_id TEXT,
                messages TEXT,
                brief_count INTEGER DEFAULT 0,
                current_brief TEXT,
                created_at TIMESTAMP,
                ended_at TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS watchlist (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                feature_id TEXT,
                feature_name TEXT,
                cloud TEXT,
                added_at TIMESTAMP,
                last_score REAL,
                last_checked TIMESTAMP
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def save_to_history(
    user_id: str,
    session_id: str,
    messages: list,
    brief_count: int,
    current_brief: str | None,
) -> None:
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO conversation_history
            (user_id, session_id, messages, brief_count, current_brief, created_at, ended_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                session_id,
                json.dumps(messages),
                brief_count,
                current_brief or "None",
                datetime.now(timezone.utc).isoformat(),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    except Exception as e:
        print(f"❌ Error saving conversation history: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def add_to_watchlist(user_id: str, feature_id: str, feature_name: str, cloud: str) -> None:
    """Add/update one watchlist subscription for a user and feature."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id FROM watchlist
            WHERE user_id = ? AND feature_id = ? AND cloud = ?
            """,
            (user_id, feature_id, cloud),
        )
        row = cur.fetchone()
        now = datetime.now(timezone.utc).isoformat()
        if row:
            cur.execute(
                """
                UPDATE watchlist
                SET feature_name = ?, added_at = ?
                WHERE id = ?
                """,
                (feature_name, now, row[0]),
            )
        else:
            cur.execute(
                """
                INSERT INTO watchlist
                (user_id, feature_id, feature_name, cloud, added_at, last_score, last_checked)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (user_id, feature_id, feature_name, cloud, now, None, None),
            )
        conn.commit()
    finally:
        conn.close()


def remove_from_watchlist(user_id: str, feature_id: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM watchlist WHERE user_id = ? AND feature_id = ?",
            (user_id, feature_id),
        )
        conn.commit()
    finally:
        conn.close()


def is_on_watchlist(user_id: str, feature_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM watchlist WHERE user_id = ? AND feature_id = ? LIMIT 1",
            (user_id, feature_id),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def get_all_watchlist_items() -> list[dict]:
    """Fetch all watchlist rows for background checks."""
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, user_id, feature_id, feature_name, cloud, last_score
            FROM watchlist
            """
        )
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "user_id": r[1],
                "feature_id": r[2],
                "feature_name": r[3],
                "cloud": r[4],
                "last_score": r[5],
            }
            for r in rows
        ]
    finally:
        conn.close()


def get_user_watchlist(user_id: str) -> list[dict]:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, feature_id, feature_name, cloud, added_at, last_score
            FROM watchlist
            WHERE user_id = ?
            ORDER BY added_at DESC
            """,
            (user_id,),
        )
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "feature_id": r[1],
                "feature_name": r[2],
                "cloud": r[3],
                "added_at": r[4],
                "last_score": r[5],
            }
            for r in rows
        ]
    finally:
        conn.close()


def update_watchlist_score(item_id: int, current_score: float) -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE watchlist
            SET last_score = ?, last_checked = ?
            WHERE id = ?
            """,
            (float(current_score), datetime.now(timezone.utc).isoformat(), item_id),
        )
        conn.commit()
    finally:
        conn.close()


def get_current_feature_score(feature_id: str, cloud: str) -> float | None:
    """Resolve current score by feature_id from latest available FY2027 quarter."""
    try:
        result = get_adoption_heatmap_data(cloud, "FY2027")
        quarters = result.get("quarters", {})
        for q in ["Q4", "Q3", "Q2", "Q1"]:
            feats = quarters.get(q) or []
            match = next((f for f in feats if f.get("feature_id") == feature_id), None)
            if match:
                return float(match.get("score") or 0.0)
    except Exception as e:
        logger.warning("get_current_feature_score failed for %s/%s: %s", cloud, feature_id, e)
    return None


def check_watchlist_alerts(client) -> None:
    """Weekly watchlist monitor: DM when feature score drops by 15%+ versus last check."""
    items = get_all_watchlist_items()
    if not items:
        return

    for item in items:
        current_score = get_current_feature_score(item["feature_id"], item["cloud"])
        if current_score is None:
            continue

        # Demo mode: send heartbeat alert every run for easy live demo.
        if WATCHLIST_DEMO_MODE:
            last_score_demo = item.get("last_score")
            if last_score_demo is not None and last_score_demo > 0:
                delta_pct = ((current_score - last_score_demo) / last_score_demo) * 100.0
                trend_txt = (
                    f"↑ +{delta_pct:.0f}%" if delta_pct > 0
                    else f"↓ {delta_pct:.0f}%" if delta_pct < 0
                    else "→ 0%"
                )
            else:
                trend_txt = "→ baseline set"
            try:
                client.chat_postMessage(
                    channel=item["user_id"],
                    text=(
                        f":bell: *Watchlist Demo Alert: {item['feature_name']}*\n"
                        f"Current adoption: `{current_score:.0f}%`\n"
                        f"Cloud: {item['cloud']} · Trend: {trend_txt}"
                    ),
                )
            except Exception as e:
                logger.warning("watchlist demo alert DM failed: %s", e)
            update_watchlist_score(item["id"], current_score)
            continue
        if current_score <= WATCHLIST_MIN_SCORE:
            update_watchlist_score(item["id"], current_score)
            continue

        last_score = item.get("last_score")
        if last_score is not None and last_score > 0:
            drop_ratio = (current_score - last_score) / last_score
            if current_score < last_score * (1 - WATCHLIST_THRESHOLD / 100.0):
                try:
                    client.chat_postMessage(
                        channel=item["user_id"],
                        text=(
                            f":warning: *Watchlist Alert: {item['feature_name']}*\n"
                            f"Adoption dropped from `{last_score:.0f}%` -> `{current_score:.0f}%`\n"
                            f"Cloud: {item['cloud']} · Trend: ↓ {abs(drop_ratio) * 100:.0f}%"
                        ),
                    )
                except Exception as e:
                    logger.warning("watchlist alert DM failed: %s", e)

        update_watchlist_score(item["id"], current_score)


def send_daily_pulse(client):
    """Send personalized daily pulse to all users with watchlist items."""
    all_watchlist_users = sorted({item["user_id"] for item in get_all_watchlist_items()})

    for user_id in all_watchlist_users:
        try:
            items = get_user_watchlist(user_id)
            if not items:
                continue

            blocks = [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": "📊 Your Daily Adoption Pulse"},
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": (
                                f"_{len(items)} watched feature"
                                f"{'s' if len(items) != 1 else ''} · "
                                f"{datetime.now().strftime('%B %d, %Y')}_"
                            ),
                        }
                    ],
                },
                {"type": "divider"},
            ]

            for item in items:
                try:
                    result = get_adoption_heatmap_data(item["cloud"], "FY2027")
                    quarters = result.get("quarters", {})
                    features = next(
                        (quarters[q] for q in ["Q4", "Q3", "Q2", "Q1"] if quarters.get(q)),
                        [],
                    )
                    matched = next(
                        (f for f in features if f.get("feature_id") == item["feature_id"]),
                        None,
                    )
                    if not matched:
                        continue

                    score = float(matched.get("score") or 0)
                    last_score = float(item.get("last_score") or 0)
                    mau = int(matched.get("mau") or 0)

                    health = (
                        ":large_green_circle:" if score > 20
                        else ":large_yellow_circle:" if score >= 5
                        else ":red_circle:"
                    )

                    if last_score > 0:
                        delta = score - last_score
                        change = (
                            f"↑ +{delta:.0f}pts" if delta > 2
                            else f"↓ {delta:.0f}pts" if delta < -2
                            else "→ No change"
                        )
                    else:
                        change = "First check"

                    mau_display = f"{mau/1000:.1f}K" if mau >= 1000 else str(mau)

                    blocks.append(
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": (
                                    f"{health} *{item['feature_name']}*\n"
                                    f"`{score:.0f}%` adoption  ·  {mau_display} MAU  ·  {change}"
                                ),
                            },
                        }
                    )
                    blocks.append(
                        {
                            "type": "actions",
                            "elements": [
                                {
                                    "type": "button",
                                    "text": {"type": "plain_text", "text": "View Detail"},
                                    "action_id": "heatmap_feature_detail",
                                    "value": (
                                        f"{item['feature_id']}|{item['feature_name']}|"
                                        f"{item['cloud']}|FY2027"
                                    ),
                                    "style": "primary",
                                },
                                {
                                    "type": "button",
                                    "text": {"type": "plain_text", "text": "❌ Remove"},
                                    "action_id": "remove_from_watchlist",
                                    "value": (
                                        f"{item['feature_id']}|{item['feature_name']}|"
                                        f"{item['cloud']}"
                                    ),
                                    "style": "danger",
                                },
                            ],
                        }
                    )

                    if last_score > 0 and score < last_score * 0.85:
                        blocks.append(
                            {
                                "type": "context",
                                "elements": [
                                    {
                                        "type": "mrkdwn",
                                        "text": (
                                            f":warning: Dropped "
                                            f"{((score-last_score)/last_score*100):.0f}% "
                                            f"since last check — action needed!"
                                        ),
                                    }
                                ],
                            }
                        )

                    update_watchlist_score(item["id"], score)

                    blocks.append({"type": "divider"})
                except Exception as e:
                    logger.warning("Pulse failed for %s: %s", item.get("feature_name"), e)
                    continue

            blocks.append(
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "_Use `/feature-watchlist` to manage your watchlist_",
                        }
                    ],
                }
            )

            client.chat_postMessage(
                channel=user_id,
                blocks=blocks,
                text=f"📊 Your Daily Adoption Pulse — {len(items)} features tracked",
            )
        except Exception as e:
            logger.warning("Daily pulse failed for %s: %s", user_id, e)


def get_session(user_id: str) -> dict:
    if user_id not in active_sessions:
        briefs = server.get_user_briefs(user_id)
        brief_names = list(briefs.keys())
        current_brief = user_last_brief.get(user_id) or (brief_names[-1] if brief_names else None)
        active_sessions[user_id] = {
            "session_id": f"session_{user_id}_{int(datetime.now().timestamp())}",
            "messages": [],
            "current_brief": current_brief,
            "brief_count": len(briefs),
        }
    return active_sessions[user_id]


def clear_session(user_id: str) -> None:
    session = active_sessions.pop(user_id, None)
    if not session:
        return
    save_to_history(
        user_id=user_id,
        session_id=session.get("session_id", "unknown"),
        messages=session.get("messages", []),
        brief_count=session.get("brief_count", 0),
        current_brief=session.get("current_brief"),
    )


def format_for_slack(text: str) -> str:
    if not text:
        return text

    text = text.replace("\r\n", "\n")
    text = re.sub(r"\*\*([^*]+)\*\*", r"*\1*", text)  # markdown bold -> slack bold
    text = re.sub(r"(?m)^\s*-\s+", "• ", text)  # "- " bullets -> "• "
    text = re.sub(r"\n([\d]+\.)", r"\n\n\1", text)  # spacing before numbered lists
    text = re.sub(r"\n•", r"\n  •", text)  # indent bullets

    # Bold first header-like line ending with ":"
    lines = text.split("\n")
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("*") and stripped.endswith("*"):
            break
        if stripped.endswith(":"):
            lines[i] = f"*{stripped}*"
        break
    return "\n".join(lines)


def split_into_chunks(text: str, max_length: int = 3900) -> list[str]:
    if len(text) <= max_length:
        return [text]

    chunks: list[str] = []
    current = ""
    paragraphs = text.split("\n\n")

    for para in paragraphs:
        if len(current) + len(para) + 2 > max_length:
            if current:
                chunks.append(current.strip())
                current = para + "\n\n"
            else:
                # paragraph too long: split on sentences
                sentences = para.split(". ")
                for s in sentences:
                    piece = s if s.endswith(".") else s + "."
                    if len(current) + len(piece) + 1 > max_length:
                        chunks.append(current.strip())
                        current = piece + " "
                    else:
                        current += piece + " "
        else:
            current += para + "\n\n"

    if current.strip():
        chunks.append(current.strip())
    return chunks


# -------------------------
# Conversational AI - Adoption intent keywords
# -------------------------
ADOPTION_KEYWORDS = [
    "adoption", "heatmap", "b2b commerce", "b2b",
    "cart", "checkout", "pricing", "search",
    "shipping", "payments", "buyer groups",
    "buyer messaging", "promotions", "subscriptions",
    "tax", "agentforce for shopping", "pft",
    "feature group", "mau", "movers", "declining",
    "analytics", "buyer", "commerce adoption"
]


def _is_adoption_query(text: str) -> bool:
    """Quick check before calling LLM - commerce-specific keywords only."""
    text_lower = text.lower()
    return any(kw in text_lower for kw in ADOPTION_KEYWORDS)


# -------------------------
# Slack events / actions
# -------------------------
@app.event("message")
def handle_message(event, say, client):
    if event.get("bot_id") or event.get("subtype"):
        return

    text = event.get("text", "").strip()
    channel = event.get("channel")
    thread_ts = event.get("thread_ts")

    # Heatmap thread-reply flow (only for replies in tracked heatmap threads)
    if thread_ts and channel:
        ctx = HEATMAP_CONTEXT.get(channel)
        if ctx and ctx.get("ts") == thread_ts:
            features = ctx.get("features", [])
            cloud = ctx.get("cloud", "")
            fy = ctx.get("fy", "")
            text_lower = text.lower()
            matches = [
                f for f in features
                if text_lower in f.get("feature", "").lower()
                or f.get("feature_group", "").lower() in text_lower
            ]

            if not matches:
                feature_names = [f["feature"] for f in features[:10]]
                say(
                    thread_ts=thread_ts,
                    text=(
                        f":mag: Feature *'{text}'* not found.\n"
                        f"Try one of: {', '.join(feature_names)}"
                    ),
                )
                return

            best = max(matches, key=lambda f: f.get("score", 0))
            feature_name = best["feature"]
            try:
                best_feature_id = best.get("feature_id")
                portfolio, family = _CLOUD_MAPPING.get(cloud, ("Commerce", "B2B Commerce"))

                movers_data = get_feature_account_movers(
                    feature_id=best_feature_id,
                    snapshot_date=best.get("data_dt") or "",
                    portfolio=portfolio,
                    family=family,
                )
                blocks, color = build_feature_detail_blocks(
                    feature=best,
                    movers=movers_data,
                    cloud=cloud,
                    fy=fy,
                    call_llm_fn=server.call_llm_gateway_with_retry,
                    user_id=event.get("user"),
                    is_on_watchlist_fn=is_on_watchlist,
                )

                client.chat_postMessage(
                    channel=channel,
                    text=f":mag: *{feature_name}* — drill-down",
                    thread_ts=thread_ts,
                    attachments=[{"color": color, "blocks": blocks}]
                )
                print(f"Drilldown posted for {feature_name} in {cloud} {fy}")
            except Exception as e:
                print(f"Drilldown failed for {feature_name}: {e}")
                say(
                    thread_ts=thread_ts,
                    text=f":x: Drill-down failed for *{feature_name}*: {str(e)}",
                )
            return

    # -------------------------
    # Conversational adoption query handling (new in adoption-claude branch)
    # -------------------------
    user = event["user"]
    ts = event.get("ts", "")

    # Only respond to adoption queries in DMs or when bot is mentioned
    is_dm = channel.startswith("D")

    # Check if bot is mentioned (need to get bot user ID first)
    is_mentioned = False
    try:
        auth_response = client.auth_test()
        bot_user_id = auth_response.get("user_id", "")
        is_mentioned = f"<@{bot_user_id}>" in text if bot_user_id else False
    except Exception:
        bot_user_id = ""
        is_mentioned = False

    # If this is a potential adoption query and in the right context (DM or mentioned)
    if (is_dm or is_mentioned) and _is_adoption_query(text):
        # Show typing indicator
        try:
            client.reactions_add(channel=channel, timestamp=ts, name="hourglass_flowing_sand")
        except Exception:
            pass  # Reaction add might fail if message is too old

        def handle_adoption_query():
            """Background thread for adoption query handling."""
            try:
                # Classify intent using LLM gateway
                from server import call_llm_gateway_with_retry
                intent = classify_adoption_intent(text, call_llm_fn=call_llm_gateway_with_retry)

                if intent["type"] == "not_adoption":
                    # Not an adoption query - let it fall through to normal handling
                    try:
                        client.reactions_remove(
                            channel=channel, timestamp=ts, name="hourglass_flowing_sand"
                        )
                    except Exception:
                        pass
                    return

                cloud = intent.get("cloud") or "Commerce B2B"
                fy = intent.get("fy") or "FY2027"

                # Route based on intent type
                if intent["type"] == "heatmap_summary":
                    # Generate full heatmap
                    result = get_adoption_heatmap_data(cloud, fy)
                    quarters = result.get("quarters", {})
                    features = []
                    for q in ["Q4", "Q3", "Q2", "Q1"]:
                        if quarters.get(q):
                            features = quarters[q]
                            break

                    if features:
                        blocks = build_adoption_heatmap_blocks(features, cloud, fy)
                        client.chat_postMessage(
                            channel=channel,
                            thread_ts=ts,
                            blocks=blocks,
                            text=f"{cloud} adoption summary for {fy}"
                        )

                        # Store context so thread replies work
                        HEATMAP_CONTEXT[channel] = {
                            "cloud":    cloud,
                            "fy":       fy,
                            "industry": None,
                            "region":   None,
                            "features": features,  # all features
                            "ts":       ts,        # root message ts
                            "created":  datetime.now(tz=timezone.utc).timestamp(),
                        }
                        print(
                            f"Conversational context stored: {cloud} heatmap summary "
                            f"— {len(features)} features"
                        )
                    else:
                        client.chat_postMessage(
                            channel=channel,
                            thread_ts=ts,
                            text=f":mag: No adoption data found for {cloud} {fy}"
                        )

                elif intent["type"] == "group_drilldown":
                    group = intent.get("feature_group")
                    if not group:
                        client.chat_postMessage(
                            channel=channel,
                            thread_ts=ts,
                            text=(
                                "Which feature group would you like to explore?\n\n"
                                "Options: Cart, Search, Pricing, Checkout, Shipping, "
                                "Payments, Buyer Groups, Promotions, Subscriptions, etc."
                            )
                        )
                        return

                    # Get data and filter for the feature group
                    result = get_adoption_heatmap_data(cloud, fy)
                    quarters = result.get("quarters", {})
                    all_features = []
                    for q in ["Q4", "Q3", "Q2", "Q1"]:
                        if quarters.get(q):
                            all_features = quarters[q]
                            break

                    # Filter for matching group (case-insensitive)
                    group_lower = group.lower()
                    matching = [
                        f for f in all_features
                        if f.get("feature_group", "").lower() == group_lower
                    ]

                    if matching:
                        blocks = build_group_drilldown_blocks(matching, group, cloud, fy)
                        client.chat_postMessage(
                            channel=channel,
                            thread_ts=ts,
                            blocks=blocks,
                            text=f"{group} drill-down for {cloud} {fy}"
                        )

                        # Store context so thread replies work
                        HEATMAP_CONTEXT[channel] = {
                            "cloud":    cloud,
                            "fy":       fy,
                            "industry": None,
                            "region":   None,
                            "features": matching,  # features for this group
                            "ts":       ts,        # root message ts
                            "created":  datetime.now(tz=timezone.utc).timestamp(),
                        }
                        print(
                            f"Conversational context stored: {group} "
                            f"— {len(matching)} features"
                        )
                    else:
                        client.chat_postMessage(
                            channel=channel,
                            thread_ts=ts,
                            text=f":mag: No data found for feature group *{group}* in {cloud} {fy}"
                        )

                elif intent["type"] == "feature_detail":
                    feature = intent.get("feature")
                    if not feature:
                        client.chat_postMessage(
                            channel=channel,
                            thread_ts=ts,
                            text=(
                                "Which specific feature would you like details on?\n"
                                "Try: 'Show me Cart Domain' or 'Tell me about Checkout API'"
                            )
                        )
                        return

                    # Search for the feature
                    result = get_adoption_heatmap_data(cloud, fy)
                    quarters = result.get("quarters", {})
                    all_features = []
                    for q in ["Q4", "Q3", "Q2", "Q1"]:
                        if quarters.get(q):
                            all_features = quarters[q]
                            break

                    feature_lower = feature.lower()
                    matches = [
                        f for f in all_features
                        if feature_lower in f.get("feature", "").lower()
                    ]

                    if matches:
                        best = max(matches, key=lambda f: f.get("score", 0))
                        portfolio, family = _CLOUD_MAPPING.get(cloud, ("Commerce", "B2B Commerce"))

                        try:
                            movers_data = get_feature_account_movers(
                                feature_id=best.get("feature_id"),
                                snapshot_date=best.get("data_dt") or "",
                                portfolio=portfolio,
                                family=family,
                            )
                            blocks, color = build_feature_detail_blocks(
                                feature=best,
                                movers=movers_data,
                                cloud=cloud,
                                fy=fy,
                                call_llm_fn=server.call_llm_gateway_with_retry,
                                user_id=user,
                                is_on_watchlist_fn=is_on_watchlist,
                            )
                            client.chat_postMessage(
                                channel=channel,
                                thread_ts=ts,
                                attachments=[{"color": color, "blocks": blocks}],
                                text=f":mag: *{best['feature']}* — feature detail"
                            )
                        except Exception as e:
                            print(f"Feature detail failed: {e}")
                            client.chat_postMessage(
                                channel=channel,
                                thread_ts=ts,
                                text=f":x: Failed to get details for *{best['feature']}*"
                            )
                    else:
                        client.chat_postMessage(
                            channel=channel,
                            thread_ts=ts,
                            text=f":mag: Feature *{feature}* not found in {cloud} {fy}"
                        )

                elif intent["type"] == "top_movers":
                    group = intent.get("feature_group")
                    response_text = "Top movers analysis coming soon!"
                    if group:
                        response_text = f"Top movers for *{group}* coming soon!"

                    client.chat_postMessage(
                        channel=channel,
                        thread_ts=ts,
                        text=f":chart_with_upwards_trend: {response_text}\n\n"
                             "This will show accounts with the biggest adoption changes."
                    )

                elif intent["type"] == "feature_owner":
                    feature = intent.get("feature") or intent.get("feature_group")
                    owner_text = f"Looking up owner for *{feature}*..." if feature else "Owner lookup coming soon!"

                    client.chat_postMessage(
                        channel=channel,
                        thread_ts=ts,
                        text=f":bust_in_silhouette: {owner_text}\n\n"
                             "This will show the PM and engineering owner."
                    )

                elif intent["type"] in ["industry_filter", "region_filter"]:
                    filter_val = intent.get("industry") or intent.get("region")
                    filter_type = "industry" if intent["type"] == "industry_filter" else "region"

                    client.chat_postMessage(
                        channel=channel,
                        thread_ts=ts,
                        text=(
                            f":mag: Filtering by {filter_type}: *{filter_val}*\n\n"
                            f"This will show {cloud} adoption data filtered for {filter_val}.\n"
                            "(Region/industry filters coming soon — currently showing all data)"
                        )
                    )
                else:
                    # Unknown intent type
                    client.chat_postMessage(
                        channel=channel,
                        thread_ts=ts,
                        text=(
                            ":thinking_face: I'm not sure how to help with that.\n\n"
                            "Try asking:\n"
                            "• 'Show me Commerce B2B adoption'\n"
                            "• 'How is Cart performing?'\n"
                            "• 'What are the top movers in Search?'"
                        )
                    )

            except Exception as e:
                print(f"Adoption query handler error: {e}")
                log_error(f"Conversational adoption error: {e}")
                try:
                    client.chat_postMessage(
                        channel=channel,
                        thread_ts=ts,
                        text=(
                            ":x: Something went wrong processing your adoption query.\n\n"
                            "Try using `/adoption-heatmap Commerce B2B` instead."
                        )
                    )
                except Exception:
                    pass

            finally:
                # Remove typing indicator
                try:
                    client.reactions_remove(
                        channel=channel, timestamp=ts, name="hourglass_flowing_sand"
                    )
                except Exception:
                    pass

        # Run adoption query in background thread
        threading.Thread(target=handle_adoption_query, daemon=True).start()
        return  # Don't fall through to normal message handling

    # -------------------------
    # Normal message handling continues below
    # -------------------------
    session = get_session(user)
    session["messages"].append({"timestamp": event.get("ts"), "text": text, "user": user})

    if text.lower() in {"end session", "clear", "reset", "start over", "new session"}:
        clear_session(user)

        try:
            user_info = client.users_info(user=user)
            first_name = user_info["user"]["profile"].get("first_name", "there")
        except Exception:
            first_name = "there"

        from agent import build_home_view

        hub_view = build_home_view(user, first_name)

        try:
            client.chat_postMessage(
                channel=user,
                text=":brain: PM Intelligence Hub",
                blocks=hub_view["blocks"],
            )
        except Exception as e:
            print(f"Error posting hub to DM: {e}")
            say(
                ":white_check_mark: Session ended and archived.\n\n"
                "You can upload a new product brief PDF or just start asking new questions."
            )

        try:
            update_home_tab(client=client, event={"user": user, "tab": "home"})
        except Exception as e:
            print(f"Could not refresh home tab: {e}")

        return

    if not text or text.lower() in {"help", "hi", "hello", "hey"}:
        briefs = server.get_user_briefs(user)
        if briefs:
            active = user_last_brief.get(user, list(briefs.keys())[0])
            say(
                "Hey! 👋 Welcome back!\n\n"
                f"You have *{len(briefs)}* brief(s) uploaded.\n"
                f"Currently discussing: *{active}*\n\n"
                "*What do you want to explore?*\n"
                "• Adoption risks\n"
                "• Renewal forecast\n"
                "• Feature usage gaps\n"
                "• V2MoM progress\n"
                "• Top accounts needing attention\n\n"
                "No commands needed - just ask naturally!"
            )
        else:
            say(
                "Hey! 👋 I'm your *Adoption Intelligence Bot*.\n\n"
                "Upload a product brief PDF and I'll help you analyze it."
            )
        return

    if "list" in text.lower() and ("brief" in text.lower() or "document" in text.lower()):
        say(server.list_briefs(user_id=user))
        return

    briefs = server.get_user_briefs(user)
    if not briefs:
        say("📭 I don't have any briefs from you yet!\n\nUpload a PDF to get started.")
        return

    if user not in user_last_brief:
        user_last_brief[user] = list(briefs.keys())[-1]
        save_last_briefs()

    switch_phrases = ["let's talk about", "switch to", "use", "discuss", "analyze"]
    if any(p in text.lower() for p in switch_phrases):
        for bn in briefs.keys():
            if bn.lower() in text.lower():
                user_last_brief[user] = bn
                save_last_briefs()
                session["current_brief"] = bn
                say(f"✅ Switched to *{bn}*. What would you like to know?")
                return

    brief_name = user_last_brief[user]
    session["current_brief"] = brief_name

    try:
        say("🤖 Analyzing...")
        result = server.query_brief(brief_name, text, user_id=user)
        result = format_for_slack(result)

        if len(result) > 3900:
            chunks = split_into_chunks(result, 3900)
            for i, chunk in enumerate(chunks):
                if i == 0:
                    say(chunk)
                else:
                    say(f"_(continued {i+1}/{len(chunks)})_\n\n{chunk}")
        else:
            say(result)
    except Exception as e:
        say(f"❌ Error: {str(e)}")


@app.event("app_home_opened")
def update_home_tab(client, event, logger=None):
    del logger
    if event.get("tab") != "home":
        return
    user_id = event["user"]

    # Don't overwrite an active heatmap view
    if _home_state_get(user_id).get("status") in ("loading", "heatmap_loaded"):
        return
    client.api_call(
        "views.publish",
        json={
            "user_id": user_id,
            "view": {
                "type": "home",
                "blocks": build_home_initial_blocks(),
            },
        },
    )


@app.action("refresh_app_home")
def handle_refresh_app_home(ack, body, client):
    ack()
    reload_thresholds()
    user_id = body["user"]["id"]
    client.api_call("views.publish", json={
        "user_id": user_id,
        "view": {"type": "home", "blocks": build_home_initial_blocks()}
    })


@app.action("home_module_select")
def handle_module_select(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    _home_state_set(user_id, status="initial")  # reset when user navigates
    selected = body["actions"][0]["selected_option"]["value"]

    if selected == "adoption":
        blocks = build_home_initial_blocks() + [
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Select a cloud:*"},
                "accessory": {
                    "type": "static_select",
                    "placeholder": {
                        "type": "plain_text",
                        "text": "Choose cloud...",
                    },
                    "action_id": "home_cloud_select",
                    "options": HOME_ADOPTION_CLOUD_OPTIONS,
                },
            },
        ]
    elif selected == "attrition":
        blocks = build_home_initial_blocks() + build_attrition_home_blocks()
    else:
        blocks = build_home_initial_blocks()

    client.api_call("views.publish", json={
        "user_id": user_id,
        "view": {"type": "home", "blocks": blocks}
    })


@app.action("home_cloud_select")
def handle_cloud_select(ack, body, client):
    ack()
    user_id = body["user"]["id"]

    # Debounce — prevent double trigger
    now_ts = time.time()
    last = _LAST_CLOUD_SELECT.get(user_id, 0)
    if now_ts - last < 3:  # debounce 3 seconds
        return
    _LAST_CLOUD_SELECT[user_id] = now_ts

    # Prevent concurrent loads
    if _home_state_get(user_id).get("status") == "loading":
        return
    _home_state_set(user_id, status="loading")

    action = body["actions"][0]
    cloud_input = action.get("value")
    if not cloud_input:
        cloud_input = action.get("selected_option", {}).get("value", "Commerce B2B")
    cloud = resolve_cloud_key(cloud_input)
    fy = "FY2027"

    client.api_call("views.publish", json={
        "user_id": user_id,
        "view": {"type": "home", "blocks": build_home_loading_blocks(cloud)}
    })

    def _load():
        try:
            result = get_adoption_heatmap_data(cloud, fy)
            quarters = result.get("quarters", {})
            features = next(
                (quarters[q] for q in ["Q4", "Q3", "Q2", "Q1"] if quarters.get(q)),
                [],
            )

            if not features:
                heatmap_body = [
                    {"type": "divider"},
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"*📊 {cloud} · {result.get('fy') or fy}*\n"
                                "_No adoption data in the selected window._"
                            ),
                        },
                    },
                ]
                blocks = build_home_initial_blocks() + heatmap_body
            else:
                groups = defaultdict(list)
                for f in features:
                    groups[f.get("feature_group", "Unknown")].append(f)

                # Red ≤5% · watch 6–19% (strictly, (5, 20) non-green) · green ≥20%
                t = {"green": 20, "red": 5, "watch_lo": 6, "watch_hi": 19}

                def _group_avg(feats: list) -> float:
                    return sum(float(f.get("score", 0) or 0) for f in feats) / len(
                        feats
                    )

                def _band(avg: float) -> str:
                    if avg >= t["green"]:
                        return "healthy"
                    if avg <= t["red"]:
                        return "critical"
                    return "watch"  # 5 < avg < 20 (covers 6–19%)

                healthy_count = sum(
                    1 for g in groups.values() if _band(_group_avg(g)) == "healthy"
                )
                watch_count = sum(
                    1 for g in groups.values() if _band(_group_avg(g)) == "watch"
                )
                critical_count = sum(
                    1 for g in groups.values() if _band(_group_avg(g)) == "critical"
                )

                snapshot_date = (
                    result.get("summary", {}).get("latest_dt", "") or "latest"
                )
                total_accounts = result.get("summary", {}).get("total_accounts", 0)
                if total_accounts in (None, 0):
                    total_accounts = max(
                        (f.get("account_count", 0) for f in features), default=0
                    )
                total_accounts = int(total_accounts or 0)

                blocks = [
                    {
                        "type": "header",
                        "text": {
                            "type": "plain_text",
                            "text": f"📊 {cloud} · Adoption Heatmap",
                        },
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": (
                                    f"{total_accounts:,} accounts · {len(features)} features · "
                                    f"{snapshot_date}"
                                ),
                            }
                        ],
                    },
                    {"type": "divider"},
                    {
                        "type": "section",
                        "fields": [
                            {
                                "type": "mrkdwn",
                                "text": f":large_green_circle: *{healthy_count}* Healthy",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f":large_yellow_circle: *{watch_count}* Watch",
                            },
                            {
                                "type": "mrkdwn",
                                "text": f":red_circle: *{critical_count}* Critical",
                            },
                        ],
                    },
                    {
                        "type": "context",
                        "elements": [
                            {
                                "type": "mrkdwn",
                                "text": (
                                    f":large_green_circle: ≥{t['green']}% Healthy  ·  "
                                    f":large_yellow_circle: {t['watch_lo']}-{t['watch_hi']}% Watch  ·  "
                                    f":red_circle: ≤{t['red']}% Critical"
                                ),
                            }
                        ],
                    },
                    {"type": "divider"},
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "🔍 Drill into groups",
                                },
                                "action_id": "home_drill_groups",
                                "value": cloud,
                            },
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "🔄 Refresh",
                                },
                                "action_id": "home_cloud_select",
                                "value": cloud,
                                "style": "primary",
                            },
                        ],
                    },
                ]
            logger.warning("HOME heatmap published blocks=%s", len(blocks))
            client.api_call(
                "views.publish",
                json={
                    "user_id": user_id,
                    "view": {
                        "type": "home",
                        "blocks": blocks,
                    },
                },
            )
            _home_state_set(user_id, status="heatmap_loaded")
            st = HOME_STATE.get(user_id)
            if isinstance(st, dict):
                st.pop("canvas_id", None)
        except Exception as e:
            logger.warning(f"FULL ERROR: {e}")
            logger.warning(f"Heatmap load failed: {e}")
            _home_state_set(user_id, status="initial")
            client.api_call("views.publish", json={
                "user_id": user_id,
                "view": {
                    "type": "home",
                    "blocks": build_home_initial_blocks(),
                }
            })
            return

    threading.Thread(target=_load, daemon=True).start()


@app.action("home_drill_groups")
def handle_home_drill_groups(ack, body, client):
    """Post a DM with feature group averages (from App Home)."""
    ack()
    user_id = body["user"]["id"]
    raw = (body.get("actions") or [{}])[0].get("value", "Commerce B2B")
    cloud = resolve_cloud_key(str(raw).strip() or "Commerce B2B")
    fy = "FY2027"
    try:
        result = get_adoption_heatmap_data(cloud, fy)
        quarters = result.get("quarters", {})
        features = next(
            (quarters[q] for q in ["Q4", "Q3", "Q2", "Q1"] if quarters.get(q)),
            [],
        )
        if not features:
            client.chat_postMessage(
                channel=user_id,
                text=f"No adoption data to drill for *{cloud}*.",
            )
            return
        groups = defaultdict(list)
        for f in features:
            groups[f.get("feature_group", "Unknown")].append(f)
        rows = []
        for name, feats in groups.items():
            avg = sum(float(f.get("score", 0) or 0) for f in feats) / len(feats)
            rows.append((name, avg, len(feats)))
        rows.sort(key=lambda x: -x[1])
        lines = [f"• *{n}* — avg *{a:.0f}%* · {c} feature(s)" for n, a, c in rows[:40]]
        msg = f"*Feature groups* · {cloud} · {fy}\n" + "\n".join(lines)
        if len(rows) > 40:
            msg += f"\n_… and {len(rows) - 40} more_"
        client.chat_postMessage(channel=user_id, text=msg[:4000])
    except Exception as e:
        logger.warning("home_drill_groups: %s", e)
        client.chat_postMessage(
            channel=user_id,
            text=f"Could not load group drilldown: {e}",
        )


@app.action("run_gm_review_commerce")
def handle_run_commerce(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    client.chat_postMessage(
        channel=user_id,
        text="⏳ Running Commerce Cloud GM Review... I'll notify you when it's ready!",
    )


@app.action("run_gm_review_fsc")
def handle_run_fsc(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    client.chat_postMessage(
        channel=user_id,
        text="⏳ Running FSC GM Review... I'll notify you when it's ready!",
    )


@app.action("upload_product_brief")
def handle_upload_button(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    client.chat_postMessage(
        channel=user_id,
        text="📄 *Ready to upload a product brief!*\n\nJust drag and drop a PDF file here and I'll analyze it for you.",
    )


@app.action("upload_new_brief")
def handle_upload_new_button(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    client.chat_postMessage(
        channel=user_id,
        text="📄 *Upload another product brief*\n\nDrag and drop a PDF file and I'll add it to your collection.",
    )


@app.action("module_attrition")
def handle_module_attrition(ack, body, client):
    """Handle Attrition Risk Predictor module selection."""
    ack()
    user_id = body["user"]["id"]
    client.chat_postMessage(
        channel=user_id,
        text=(
            ":chart_with_upwards_trend: *Attrition Risk Predictor Module*\n\n"
            "Look up churn risk by account and product line.\n\n"
            "*Available Commands:*\n"
            "- `/attrition-risk` — Analyze specific accounts\n"
            "- `/attrition-clouds` — View product-level risk patterns\n"
            "- `/gm-review-canvas` — GM review for at-risk renewals\n"
            "- `/at-risk-canvas` — At-risk account canvas\n\n"
            "Try: `/attrition-risk Acme Corp`"
        ),
    )


@app.action("module_feature_usage")
def handle_module_feature_usage(ack, body, client):
    """Handle Feature Usage Scorecard module selection."""
    ack()
    user_id = body["user"]["id"]
    client.chat_postMessage(
        channel=user_id,
        text=(
            ":chart_with_upwards_trend: *Feature Usage Scorecard Module*\n\n"
            "Score feature adoption across your customer base.\n\n"
            "Ask me things like:\n"
            "- What's the adoption rate for [feature]?\n"
            "- Which accounts have low usage?\n"
            "- Show me feature usage trends\n\n"
            "This module integrates with your Snowflake analytics data!"
        ),
    )


@app.action("btn_end_session_home")
def handle_end_session_home(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    clear_session(user_id)
    update_home_tab(client, {"user": user_id, "tab": "home"}, logger=None)
    client.chat_postMessage(
        channel=user_id,
        text="✅ Session ended and archived.\n\nYou can upload a new product brief PDF or just start asking new questions.",
    )


@app.action("quick_adoption_risks")
def handle_quick_adoption_risks(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    brief_name = body["actions"][0]["value"]
    client.chat_postMessage(channel=user_id, text="🤖 Analyzing adoption risks...")
    result = server.query_brief(
        brief_name,
        "What are the top 3 adoption risks mentioned in this product brief? For each risk, explain the potential impact and suggested mitigation strategies.",
        user_id=user_id,
    )
    client.chat_postMessage(channel=user_id, text=format_for_slack(result))


@app.action("quick_big_rocks")
def handle_quick_big_rocks(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    brief_name = body["actions"][0]["value"]
    client.chat_postMessage(channel=user_id, text="🤖 Extracting Big Rocks...")
    result = server.query_brief(
        brief_name,
        "Analyze the product brief to extract the top 3 'Big Rocks' and describe the intended customer value (JTBD format).",
        user_id=user_id,
    )
    client.chat_postMessage(channel=user_id, text=format_for_slack(result))


@app.action("quick_metrics")
def handle_quick_metrics(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    brief_name = body["actions"][0]["value"]
    client.chat_postMessage(channel=user_id, text="🤖 Analyzing success metrics...")
    result = server.query_brief(
        brief_name,
        "What are the key success metrics and KPIs mentioned in this brief? How will success be measured?",
        user_id=user_id,
    )
    client.chat_postMessage(channel=user_id, text=format_for_slack(result))


@app.action("quick_audience")
def handle_quick_audience(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    brief_name = body["actions"][0]["value"]
    client.chat_postMessage(channel=user_id, text="🤖 Analyzing target audience...")
    result = server.query_brief(
        brief_name,
        "Who is the target audience for this product? What are their key needs, pain points, and use cases?",
        user_id=user_id,
    )
    client.chat_postMessage(channel=user_id, text=format_for_slack(result))


@app.action("quick_summary")
def handle_quick_summary(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    brief_name = body["actions"][0]["value"]
    client.chat_postMessage(channel=user_id, text="🤖 Generating comprehensive summary...")
    result = server.query_brief(
        brief_name,
        "Provide a comprehensive executive summary of this product brief covering: product overview, target audience, key features, success metrics, adoption risks, and timeline.",
        user_id=user_id,
    )
    result = format_for_slack(result)
    if len(result) > 3900:
        chunks = split_into_chunks(result, 3900)
        for i, chunk in enumerate(chunks):
            client.chat_postMessage(
                channel=user_id,
                text=chunk if i == 0 else f"_(continued)_\n\n{chunk}",
            )
    else:
        client.chat_postMessage(channel=user_id, text=result)


@app.action(re.compile("switch_to_.*"))
def handle_switch_brief(ack, body, client, logger=None):
    ack()
    user_id = body["user"]["id"]
    brief_name = body["actions"][0]["value"]
    user_last_brief[user_id] = brief_name
    save_last_briefs()
    s = get_session(user_id)
    s["current_brief"] = brief_name
    update_home_tab(client, {"user": user_id}, logger=logger)
    client.chat_postMessage(channel=user_id, text=f"✅ Switched to *{brief_name}*\n\nWhat would you like to know?")


@app.event("app_mention")
def handle_mention(event, say):
    say("Hey! 👋 I work best in direct messages.\n\nSend me a DM and upload a product brief PDF, then ask me anything!")


ALWAYS_UPPER_ATTRITION = {
    "ssc",
    "b2b",
    "b2c",
    "oms",
    "pos",
    "apm",
    "crm",
    "erp",
    "api",
}


def smart_title_case(name: str) -> str:
    """Title-case account names; keep common acronyms uppercased (legacy /attrition-risk)."""
    name = " ".join(str(name or "").split())

    def cap_word(w: str) -> str:
        return w.upper() if w.lower() in ALWAYS_UPPER_ATTRITION else w.capitalize()

    return re.sub(r"[^\s-]+", lambda m: cap_word(m.group()), name)


def handle_list_query(text: str, user_id: str, say) -> None:
    """Show a Snowflake at-risk account list when a single-account lookup has no match."""
    from domain.analytics.snowflake_client import get_at_risk_accounts_snowflake
    from domain.salesforce.org62_client import sf_query
    from filter_parser import parse_filters

    f = parse_filters(text)
    text_lower = text.lower()
    active_filters = []

    cloud = f["cloud"]
    active_filters.append(cloud)

    risk_category = f.get("ari_filter")
    if risk_category == "High":
        active_filters.append("High Risk")
    elif risk_category == "Medium":
        active_filters.append("Medium Risk")
    elif risk_category == "Low":
        active_filters.append("Low Risk")

    if f.get("health_filter"):
        active_filters.append("Health " + f["health_filter"])
    if f.get("min_aov", 0) > 0:
        active_filters.append(
            "AOV>" + ("$1M" if f["min_aov"] >= 1000000 else "$500K")
        )

    min_attrition = 0
    if any(kw in text_lower for kw in ["over 1m", ">1m", "above 1m"]):
        min_attrition = 1000000
        active_filters.append("ATR > $1M")
    elif any(kw in text_lower for kw in ["over 500k", ">500k", "above 500k"]):
        min_attrition = 500000
        active_filters.append("ATR > $500K")
    elif any(kw in text_lower for kw in ["over 200k", ">200k", "above 200k"]):
        min_attrition = 200000
        active_filters.append("ATR > $200K")
    elif any(kw in text_lower for kw in ["over 100k", ">100k", "above 100k"]):
        min_attrition = 100000
        active_filters.append("ATR > $100K")
    elif any(kw in text_lower for kw in ["over 50k", ">50k", "above 50k"]):
        min_attrition = 50000
        active_filters.append("ATR > $50K")

    m_top = re.search(r"top\s*(\d+)", text_lower)
    limit = int(m_top.group(1)) if m_top else 25
    if any(kw in text_lower for kw in ["top 50", "all"]):
        limit = 50

    say(":hourglass: Fetching at-risk accounts from Snowflake...")

    records = get_at_risk_accounts_snowflake(
        cloud=cloud,
        risk_category=None,
        min_attrition=min_attrition,
        limit=limit,
        min_aov=f.get("min_aov") or 0,
        ari_filter=risk_category,
        sort_by=f.get("sort_by") or "atr",
    )

    if not records:
        say(
            ":x: No accounts found matching: *"
            + ", ".join(active_filters)
            + "*\n"
            + ":bulb: Try broader filters or remove the risk category filter."
        )
        return

    id_to_name: dict[str, str] = {}
    ids = list({r["account_id"] for r in records})
    try:
        for i in range(0, len(ids), 50):
            batch = ids[i : i + 50]
            id_list = "','".join(str(b) for b in batch)
            result = sf_query(
                "SELECT Id, Name FROM Account WHERE Id IN ('" + id_list + "')"
            )
            for rec in result.get("records", []):
                rid = rec.get("Id") or ""
                id_to_name[rid[:15]] = rec.get("Name", rid)
    except Exception as e:
        print("Account name lookup error: " + str(e)[:60])

    total_atr = sum(abs(r["attrition_pipeline"]) for r in records)
    filter_label = " + ".join(active_filters) if active_filters else "All Accounts"
    risk_emoji = {
        "High": ":red_circle:",
        "Medium": ":large_yellow_circle:",
        "Low": ":large_green_circle:",
    }

    lines_out = [
        ":bar_chart: *Accounts — " + filter_label + "*\n"
        "*" + str(len(records)) + " accounts | Total Predicted Attrition: $"
        + f"{total_atr:,.0f}" + "*\n"
        "_Data: Snowflake CSS · Snapshot: "
        + (records[0].get("snapshot_dt") or "N/A")
        + "_\n"
    ]

    for r in records:
        acct_id = r["account_id"]
        acct_name = id_to_name.get(str(acct_id)[:15], acct_id)
        risk = r["attrition_proba_category"]
        emoji = risk_emoji.get(risk, ":white_circle:")
        atr = abs(r["attrition_pipeline"])
        product = r["apm_lvl_3"]
        reason = r["attrition_reason"] or "N/A"
        lines_out.append(
            "- *"
            + str(acct_name)
            + "* — "
            + str(product)
            + "\n  "
            + emoji
            + " "
            + str(risk)
            + " | ATR: $"
            + f"{atr:,.0f}"
            + " | Reason: "
            + str(reason)
        )

    say("\n".join(lines_out))


@app.command("/attrition-risk")
def attrition_risk_cmd(ack, say, command, client):
    """
    Attrition risk lookup for a single account.
    Usage: /attrition-risk <Account Name>
    """
    ack()

    _this_cmd = (
        command.get("user_id"),
        (command.get("text") or "").strip(),
        int(time.time() / 3),
    )
    if getattr(attrition_risk_cmd, "_last_cmd", None) == _this_cmd:
        return
    attrition_risk_cmd._last_cmd = _this_cmd

    try:
        import threading
        import re
        from concurrent.futures import ThreadPoolExecutor

        text = command.get("text", "").strip()

        if not text:
            say(
                ":warning: Usage:\n"
                "`/attrition-risk <Account Name>`\n"
                "`/attrition-risk Commerce Cloud, Titan`\n"
                "`/attrition-risk Marketing Cloud, Acne Studios`\n"
                "`/attrition-risk 006xxxxxxxxxxxxx` (Opportunity ID)\n\n"
                ":bulb: Use `/attrition-clouds` to see all available products."
            )
            return

        def process():
            from domain.analytics.snowflake_client import (
                enrich_account_cached,
                format_enrichment_for_display,
                get_account_attrition,
                resolve_account_from_snowflake_cached,
            )
            from domain.content.canvas_builder import build_account_brief_blocks
            from domain.intelligence.risk_engine import generate_risk_analysis
            from domain.salesforce.org62_client import (
                OPPORTUNITY_RENEWAL_SOQL_FIELDS,
                _escape,
                get_red_account,
                get_renewal_opportunities,
                get_renewal_opportunities_any_cloud,
                resolve_account_enhanced,
                sf_query,
            )
            from filter_parser import parse_filters

            # Strip markdown links
            text_clean = re.sub(r"__?\[([^\]]+)\]\([^)]+\)__?", r"\1", text)
            text_clean = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text_clean)
            text_clean = text_clean.strip("_* ")

            filters = parse_filters(text_clean)
            detected_cloud = filters.get("cloud", "Commerce Cloud")

            account_parts = filters.get("manual_account_parts", [])
            # Single-account slash command: first segment after cloud (legacy behavior)
            account_search = (
                account_parts[0] if account_parts else text_clean
            )

            # Check if input is Opportunity ID
            opp_id_match = re.match(
                r"^(006[a-zA-Z0-9]{12,15})$", account_search.strip()
            )

            if opp_id_match:
                # Mode 1: Direct Opp ID lookup
                opp_id = opp_id_match.group(1)
                say(":mag: Looking up opportunity *" + opp_id + "*...")

                try:
                    result = sf_query(
                        f"SELECT {OPPORTUNITY_RENEWAL_SOQL_FIELDS} "
                        f"FROM Opportunity WHERE Id = '{_escape(opp_id)}' LIMIT 1"
                    )
                    if not result.get("records"):
                        say(":x: Opportunity *" + opp_id + "* not found in org62.")
                        return

                    opp = result["records"][0]
                    acct_data = opp.get("Account") or {}
                    account_id = acct_data.get("Id", "")
                    account_name = " ".join(
                        str(acct_data.get("Name", "Unknown") or "").split()
                    )

                    if not account_id:
                        say(":x: Opportunity has no linked account.")
                        return

                    # Detect cloud from opp name
                    opp_name = opp.get("Name", "")
                    if "B2B" in opp_name:
                        detected_cloud = "B2B Commerce"
                    elif "FSC" in opp_name or "Financial" in opp_name:
                        detected_cloud = "Financial Services Cloud"
                    else:
                        detected_cloud = "Commerce Cloud"

                    acct = {
                        "id": account_id,
                        "name": account_name,
                        "country": acct_data.get("BillingCountry", ""),
                        "opp": opp,
                    }

                except Exception as e:
                    say(":x: Error fetching opportunity: " + str(e)[:100])
                    return

            else:
                # Mode 2: Account name lookup (manual_account_parts + smart title case)
                raw_name = account_search.strip() or text_clean.strip()
                account_name_input = smart_title_case(raw_name)

                say(
                    f":mag: Looking up account *{account_name_input}* "
                    f"({detected_cloud})..."
                )
                acct = resolve_account_enhanced(
                    account_name_input, cloud=detected_cloud
                )
                if not acct:
                    snow = resolve_account_from_snowflake_cached(
                        account_name_input, cloud=detected_cloud
                    )
                    if snow:
                        acct = {
                            "id": snow.get("account_id"),
                            "name": snow.get("account_name"),
                            "opty_id": snow.get("opty_id") or "",
                            "renewal_prefetch": {
                                "renewal_aov": snow.get("renewal_aov"),
                                "renewal_atr_snow": snow.get("renewal_atr_snow"),
                                "csg_territory": snow.get("csg_territory") or "",
                                "csg_area": snow.get("csg_area") or "",
                                "csg_geo": snow.get("csg_geo") or "",
                                "target_cloud": snow.get("target_cloud") or "",
                            },
                        }

                if not acct:
                    uid = command.get("user_id") or ""
                    handle_list_query(account_name_input, uid, say)
                    return

            # Common flow
            account_id = acct["id"]
            account_name = " ".join(str(acct.get("name") or "").split())

            # Fetch opp
            if acct.get("opp"):
                opp = acct["opp"]
                opps = [opp]
            else:
                opps = get_renewal_opportunities(account_id, detected_cloud) or []
                opp = opps[0] if opps else {}

            if not opp:
                opps_any = get_renewal_opportunities_any_cloud(account_id) or []
                opp = opps_any[0] if opps_any else {}

            # Fetch red account
            red = get_red_account(account_id)

            # Snowflake enrichment (parallel)
            opty_id = opp.get("Id", "") if opp else ""
            with ThreadPoolExecutor(max_workers=2) as ex:
                fut_enrich = ex.submit(
                    enrich_account_cached, account_id, opty_id, detected_cloud
                )
                fut_attrition = ex.submit(get_account_attrition, account_id, cloud=detected_cloud)
                enrichment = fut_enrich.result()
                product_attrition = fut_attrition.result()

            display = format_enrichment_for_display(enrichment)

            # Risk analysis
            risk_notes, recommendation = generate_risk_analysis(
                account_name=account_name,
                opp=opp,
                red_account=red,
                snowflake_enrichment=enrichment,
                call_llm_fn=server.call_llm_gateway_with_retry,
            )

            health_for_tldr = display.get("health_display") or display.get(
                "ari_category", "N/A"
            )
            try:
                tldr = server.call_llm_gateway_with_retry(
                    "Summarize in 2 sentences for a PM: "
                    + "Account: "
                    + account_name
                    + " | ARI: "
                    + str(display.get("ari_category", "N/A"))
                    + " | ATR: "
                    + _resolve_atr_for_tldr(display, opp)
                    + " | Risk: "
                    + str(
                        opp.get("License_At_Risk_Reason__c") or "N/A"
                        if opp
                        else "N/A"
                    )
                    + " | Health: "
                    + str(health_for_tldr)
                    + " | Notes: "
                    + (risk_notes[:300] if risk_notes else ""),
                    system_prompt=(
                        "You are a Salesforce PM analyst. Be direct and actionable. "
                        "Max 2 sentences."
                    ),
                    max_tokens=100,
                )
                if not (tldr and str(tldr).strip()):
                    tldr = None
            except Exception:
                tldr = None

            blocks = build_account_brief_blocks(
                account={
                    "name": account_name,
                    "id": account_id,
                    "product_attrition": product_attrition,
                },
                opp=opp,
                red_account=red,
                snowflake_display=display,
                risk_notes=risk_notes,
                recommendation=recommendation,
                tldr=tldr,
                user_cloud=detected_cloud,
                all_products=product_attrition
                if isinstance(product_attrition, list)
                else [],
            )
            say(
                text="Account Risk Briefing — " + account_name,
                blocks=blocks,
            )

        threading.Thread(target=process).start()

    except Exception as e:
        say(f"❌ Error: {str(e)}")


def _parse_heatmap_filters(text: str) -> dict:
    """
    Parses slash command text into cloud, fy, industry, region.

    Examples:
        "Commerce B2B"
            → cloud="Commerce B2B", fy="FY2027",
              industry=None, region=None

        "Commerce B2B FY2027 Retail"
            → cloud="Commerce B2B", fy="FY2027",
              industry="Retail & CG", region=None

        "Commerce B2B FY2027 EMEA North"
            → cloud="Commerce B2B", fy="FY2027",
              industry=None, region="EMEA North"

        "Commerce B2B FY2027 Retail EMEA"
            → cloud="Commerce B2B", fy="FY2027",
              industry="Retail & CG", region="EMEA North"
    """
    tokens = text.strip().split()
    fy = next(
        (t.upper() for t in tokens if t.upper().startswith("FY20")),
        "FY2027"
    )
    remaining = [t for t in tokens if not t.upper().startswith("FY20")]

    # Match region — check multi-word regions first
    region = None
    for r in sorted(VALID_REGIONS, key=len, reverse=True):
        r_tokens = r.lower().split()
        for i in range(len(remaining) - len(r_tokens) + 1):
            if [t.lower() for t in remaining[i : i + len(r_tokens)]] == r_tokens:
                region = r
                remaining = remaining[:i] + remaining[i + len(r_tokens) :]
                break
        if region:
            break

    # Match industry — check multi-word industries first
    industry = None
    for ind in sorted(VALID_INDUSTRIES, key=len, reverse=True):
        ind_tokens = ind.lower().split()
        for i in range(len(remaining) - len(ind_tokens) + 1):
            if [t.lower() for t in remaining[i : i + len(ind_tokens)]] == ind_tokens:
                industry = ind
                remaining = remaining[:i] + remaining[i + len(ind_tokens) :]
                break
        if industry:
            break

    # First word of an industry (e.g. "Retail" → "Retail & CG") — prefer tokens at end
    if not industry and remaining:
        for i in range(len(remaining) - 1, -1, -1):
            tl = remaining[i].lower()
            if len(tl) < 3:
                continue
            for ind in sorted(VALID_INDUSTRIES, key=len, reverse=True):
                first_w = re.split(r"[\s,&]+", ind)[0].lower()
                if first_w and tl == first_w:
                    industry = ind
                    remaining = remaining[:i] + remaining[i + 1 :]
                    break
            if industry:
                break

    # Also match partial industry names (e.g. "Retail" → "Retail & CG")
    if not industry and remaining:
        partial = " ".join(remaining).lower()
        for ind in VALID_INDUSTRIES:
            if partial in ind.lower() or ind.lower().startswith(partial):
                # Check it's not a cloud token
                if not any(
                    partial in c.lower()
                    for c in _CLOUD_MAPPING.keys()
                ):
                    industry = ind
                    remaining = []
                    break

    # Trailing short region tokens (e.g. "EMEA" is not a full CSG name but
    # commonly typed after an industry)
    _region_short: dict[str, str] = {
        "emea": "EMEA North",
        "amer": "AMER REG",
    }
    if not region and remaining:
        last = remaining[-1].lower()
        if last in _region_short:
            region = _region_short[last]
            remaining = remaining[:-1]

    # Everything left is the cloud name
    cloud = " ".join(remaining).strip() or "Commerce B2B"

    return {
        "cloud": cloud,
        "fy": fy,
        "industry": industry,
        "region": region,
    }


def _clean_stale_heatmap_context():
    """Remove heatmap context entries older than 4 hours."""
    cutoff = datetime.now(tz=timezone.utc).timestamp() - (4 * 3600)
    stale = [
        k for k, v in HEATMAP_CONTEXT.items()
        if v.get("created", 0) < cutoff
    ]
    for k in stale:
        del HEATMAP_CONTEXT[k]


@app.command("/adoption-heatmap")
def handle_adoption_heatmap(ack, body, client, logger):
    ack()
    _clean_stale_heatmap_context()

    # Parse: cloud, FY, optional industry/region
    parsed = _parse_heatmap_filters((body.get("text") or "").strip())
    cloud_input = parsed["cloud"]
    cloud = cloud_input
    fy = parsed["fy"]
    industry = parsed["industry"]
    region = parsed["region"]
    channel = body["channel_id"]
    user = body["user_id"]

    # Resolve fuzzy cloud input to canonical PDP cloud key
    try:
        cloud = resolve_cloud_key(cloud_input)
        resolve_cloud(cloud_input)  # validate mapping tuple resolution path
    except ValueError as e:
        client.chat_postEphemeral(
            channel=channel,
            user=user,
            text=f":x: {str(e)}",
        )
        return

    # Loading indicator
    filter_parts = []
    if industry:
        filter_parts.append(industry)
    if region:
        filter_parts.append(region)
    filter_str = f"  ·  {' · '.join(filter_parts)}" if filter_parts else ""
    client.chat_postEphemeral(
        channel=channel,
        user=user,
        text=(
            f":hourglass_flowing_sand: Building heatmap for "
            f"*{cloud}* {fy}{filter_str}..."
        )
    )

    def _run_heatmap_async():
        try:
            # Generate scored features for latest quarter
            result = get_adoption_heatmap_data(
                cloud, fy, industry=industry, region=region
            )
            quarters = result.get("quarters", {})
            features = next(
                (quarters[q] for q in ["Q4", "Q3", "Q2", "Q1"]
                 if quarters.get(q)),
                []
            )
            blocks = build_adoption_heatmap_blocks(
                features, cloud, fy, industry=industry, region=region
            )

            # Post heatmap to channel
            response = client.chat_postMessage(
                channel=channel,
                text=f":bar_chart: {cloud} Adoption Heatmap · {fy}",
                blocks=blocks
            )

            # Store context keyed by channel_id
            HEATMAP_CONTEXT[channel] = {
                "cloud": cloud,
                "fy": fy,
                "industry": industry,
                "region": region,
                "features": features,
                "ts": response["ts"],
                "created": datetime.now(tz=timezone.utc).timestamp(),
            }
            logger.info(
                f"Heatmap posted for {cloud} {fy} — "
                f"{len(features)} features stored in context"
            )

        except Exception as e:
            logger.error(f"Heatmap command failed: {e}")
            client.chat_postEphemeral(
                channel=channel,
                user=user,
                text=f":x: Heatmap generation failed: {str(e)}"
            )

    threading.Thread(target=_run_heatmap_async, daemon=True).start()


@app.action("heatmap_drilldown")
def handle_heatmap_drilldown(ack, body, client, logger):
    ack()

    value = body["actions"][0]["value"]
    parts = value.split("|")
    group_name = parts[0]
    cloud = parts[1]
    fy = parts[2]
    channel = body["channel"]["id"]
    message_ts = body["message"]["ts"]

    # Get features for this group from context
    ctx = HEATMAP_CONTEXT.get(channel)
    features = ctx.get("features", []) if ctx else []
    group_features = [
        f for f in features
        if f.get("feature_group") == group_name
    ]

    if not group_features:
        client.chat_postMessage(
            channel=channel,
            thread_ts=message_ts,
            text=f":x: No features found for group *{group_name}*"
        )
        return

    # Fetch movers for worst feature in group
    worst = min(group_features, key=lambda f: f.get("score", 100))
    portfolio, family = _CLOUD_MAPPING.get(
        cloud, ("Commerce", "B2B Commerce")
    )
    movers_data = get_feature_account_movers(
        feature_id=worst.get("feature_id"),
        snapshot_date=worst.get("data_dt") or "",
        portfolio=portfolio,
        family=family,
    )

    # Build and post Layer 2 blocks
    drilldown_blocks = build_group_drilldown_blocks(
        group_features,
        group_name,
        cloud,
        fy,
        movers_data,
        user_id=body["user"]["id"],
        is_on_watchlist_fn=is_on_watchlist,
    )
    client.chat_postMessage(
        channel=channel,
        thread_ts=message_ts,
        text=f":mag: {group_name} drill-down",
        blocks=drilldown_blocks
    )
    logger.info(
        f"Group drilldown posted: {group_name} "
        f"— {len(group_features)} features, "
        f"{len(movers_data.get('top_movers', []))} movers, "
        f"{len(movers_data.get('top_losers', []))} losers"
    )


@app.action("heatmap_feature_detail")
def handle_heatmap_feature_detail(ack, body, client, logger):
    ack()

    value = body["actions"][0]["value"]
    parts = value.split("|")
    feature_id = parts[0]
    feature_nm = parts[1]
    cloud = parts[2]
    fy = parts[3]
    channel = body["channel"]["id"]
    message_ts = body["message"]["ts"]

    # Get context
    ctx = HEATMAP_CONTEXT.get(channel)
    features = ctx.get("features", []) if ctx else []

    # Find the feature
    matched = next(
        (f for f in features if f.get("feature_id") == feature_id),
        None
    )
    if not matched:
        client.chat_postMessage(
            channel=channel,
            thread_ts=message_ts,
            text=f":x: Feature *{feature_nm}* not found in context."
        )
        return

    # Fetch movers
    portfolio, family = _CLOUD_MAPPING.get(
        cloud, ("Commerce", "B2B Commerce")
    )
    movers_data = get_feature_account_movers(
        feature_id=feature_id,
        snapshot_date=matched.get("data_dt") or "",
        portfolio=portfolio,
        family=family,
    )

    # Build Layer 3 blocks
    blocks, color = build_feature_detail_blocks(
        feature=matched,
        movers=movers_data,
        cloud=cloud,
        fy=fy,
        call_llm_fn=server.call_llm_gateway_with_retry,
        user_id=body["user"]["id"],
        is_on_watchlist_fn=is_on_watchlist,
    )

    client.chat_postMessage(
        channel=channel,
        thread_ts=message_ts,
        text=f":mag: {feature_nm} · Feature detail",
        attachments=[{"color": color, "blocks": blocks}]
    )
    logger.info(
        f"Feature detail posted: {feature_nm} — "
        f"{len(movers_data.get('top_movers', []))} movers, "
        f"{len(movers_data.get('top_losers', []))} losers"
    )


@app.action("add_to_watchlist")
def handle_add_to_watchlist(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    value = body["actions"][0]["value"]

    try:
        feature_id, feature_name, cloud = value.split("|", 2)
    except ValueError:
        client.chat_postMessage(
            channel=user_id,
            text=":x: Could not parse watchlist payload.",
        )
        return

    # Save to watchlist
    add_to_watchlist(user_id, feature_id, feature_name, cloud)

    # Update originating message so button flips immediately to "Remove".
    updated_blocks_with_remove_button = body.get("message", {}).get("blocks", [])
    if updated_blocks_with_remove_button:
        for block in updated_blocks_with_remove_button:
            accessory = block.get("accessory")
            if isinstance(accessory, dict) and accessory.get("action_id") == "add_to_watchlist":
                accessory["action_id"] = "remove_from_watchlist"
                accessory["text"] = {"type": "plain_text", "text": "❌ Remove from Watchlist"}
                accessory["value"] = f"{feature_id}|{feature_name}|{cloud}"
                accessory["style"] = "danger"

            elements = block.get("elements")
            if isinstance(elements, list):
                for elem in elements:
                    if isinstance(elem, dict) and elem.get("action_id") == "add_to_watchlist":
                        elem["action_id"] = "remove_from_watchlist"
                        elem["text"] = {
                            "type": "plain_text",
                            "text": "❌ Remove from Watchlist",
                        }
                        elem["value"] = f"{feature_id}|{feature_name}|{cloud}"
                        elem["style"] = "danger"

        try:
            client.chat_update(
                channel=body["channel"]["id"],
                ts=body["message"]["ts"],
                blocks=updated_blocks_with_remove_button,
                text=f":eyes: {feature_name} added to watchlist",
            )
        except Exception as e:
            logger.warning("Failed to refresh watchlist button state: %s", e)

    # Confirm added
    client.chat_postMessage(
        channel=user_id,
        text=f":eyes: *{feature_name}* added to your watchlist!",
        blocks=[
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":eyes: *{feature_name}* added to your watchlist!\n"
                        "You'll get alerted when adoption drops significantly."
                    ),
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "❌ Remove from Watchlist"},
                        "action_id": "remove_from_watchlist",
                        "value": f"{feature_id}|{feature_name}|{cloud}",
                        "style": "danger",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📋 View My Watchlist"},
                        "action_id": "view_watchlist",
                        "value": user_id,
                    },
                ],
            },
        ],
    )

@app.action("remove_from_watchlist")
def handle_remove_from_watchlist(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    value = body["actions"][0]["value"]
    parts = value.split("|")
    feature_id = parts[0] if len(parts) > 0 else ""
    feature_name = parts[1] if len(parts) > 1 else "Unknown"

    remove_from_watchlist(user_id, feature_id)
    client.chat_postMessage(
        channel=user_id,
        text=f":x: *{feature_name}* removed from your watchlist.",
    )


@app.action("view_watchlist")
def handle_view_watchlist(ack, body, client):
    ack()
    user_id = body["user"]["id"]
    items = get_user_watchlist(user_id)

    if not items:
        client.chat_postMessage(
            channel=user_id,
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            ":eyes: Your watchlist is empty.\n"
                            "Click *👁 Watch* on any feature to start tracking it."
                        ),
                    },
                }
            ],
            text="Your watchlist is empty.",
        )
        return

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "👁 Your Watchlist"},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"{len(items)} feature{'s' if len(items) != 1 else ''} being tracked"
                    ),
                }
            ],
        },
        {"type": "divider"},
    ]
    for item in items:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":eyes: *{item['feature_name']}*\n"
                        f"{item['cloud']}  ·  Added {item['added_at']}"
                    ),
                },
            }
        )
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Detail"},
                        "action_id": "heatmap_feature_detail",
                        "value": (
                            f"{item['feature_id']}|{item['feature_name']}|"
                            f"{item['cloud']}|FY2027"
                        ),
                        "style": "primary",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "❌ Remove"},
                        "action_id": "remove_from_watchlist",
                        "value": (
                            f"{item['feature_id']}|{item['feature_name']}|{item['cloud']}"
                        ),
                        "style": "danger",
                    },
                ],
            }
        )
        blocks.append({"type": "divider"})

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        "_You'll get alerted when adoption drops significantly · "
                        "Alerts configurable via `.env`_"
                    ),
                }
            ],
        }
    )

    client.chat_postMessage(
        channel=user_id,
        blocks=blocks,
        text="Your watchlist",
    )


@app.action("account_feature_heatmap")
def handle_account_feature_heatmap(ack, body, client):
    ack()
    value = body["actions"][0]["value"]
    parts = value.split("|")
    acct_id = parts[0] if len(parts) > 0 else ""
    acct_name = parts[1] if len(parts) > 1 else "Unknown"
    cloud = parts[2] if len(parts) > 2 else "Commerce B2B"
    fy = parts[3] if len(parts) > 3 else "FY2027"
    channel = body["channel"]["id"]
    message_ts = body["message"]["ts"]

    def _load_account_heatmap():
        try:
            result = get_adoption_heatmap_data(
                cloud, fy, account_id=acct_id or None
            )
            quarters = result.get("quarters", {})
            features = next(
                (quarters[q] for q in ["Q4", "Q3", "Q2", "Q1"] if quarters.get(q)),
                [],
            )

            if not features:
                client.chat_postMessage(
                    channel=channel,
                    thread_ts=message_ts,
                    text=f":x: No feature data found for *{acct_name}*",
                )
                return

            blocks = build_adoption_heatmap_blocks(
                features, cloud, fy, title=f"📊 {acct_name} · Feature Heatmap"
            )
            client.chat_postMessage(
                channel=channel,
                thread_ts=message_ts,
                text=f"📊 {acct_name} · {cloud} Feature Heatmap",
                blocks=blocks,
            )
        except Exception as e:
            client.chat_postMessage(
                channel=channel,
                thread_ts=message_ts,
                text=f":x: Failed to load heatmap for *{acct_name}*: {str(e)}",
            )

    threading.Thread(target=_load_account_heatmap, daemon=True).start()


@app.action("heatmap_message_owner")
def handle_heatmap_message_owner(ack, body, client, logger):
    ack()
    value = body["actions"][0]["value"]
    parts = value.split("|")
    feature_nm = parts[1]
    owner = parts[2]
    cloud = parts[3]
    channel = body["channel"]["id"]
    message_ts = body["message"]["ts"]
    client.chat_postMessage(
        channel=channel,
        thread_ts=message_ts,
        text=(
            f":envelope: *Draft message to {owner}*\n\n"
            f"> Hi {owner.split()[0]}, I was reviewing adoption data "
            f"for *{feature_nm}* in {cloud} and wanted to connect. "
            f"Are you available for a quick sync this week?\n\n"
            f"_Copy and send this via Slack DM_"
        )
    )
    logger.info(f"Message owner draft posted for {owner} re: {feature_nm}")


@app.action("heatmap_compare")
def handle_heatmap_compare(ack, body, client, logger):
    ack()
    value = body["actions"][0]["value"]
    parts = value.split("|")
    feature_nm = parts[1]
    cloud = parts[2]
    channel = body["channel"]["id"]
    message_ts = body["message"]["ts"]
    client.chat_postMessage(
        channel=channel,
        thread_ts=message_ts,
        text=(
            f":bar_chart: To compare *{feature_nm}* with another feature, "
            f"reply with the feature name in this thread."
        )
    )

    ctx = HEATMAP_CONTEXT.get(channel)
    if ctx:
        result = get_adoption_heatmap_data(
            ctx.get("cloud", "Commerce B2B"),
            ctx.get("fy", "FY2027"),
            industry=ctx.get("industry"),
            region=ctx.get("region"),
        )
        quarters = result.get("quarters", {})
        all_features = next(
            (quarters[q] for q in ["Q4", "Q3", "Q2", "Q1"] if quarters.get(q)),
            [],
        )
        HEATMAP_CONTEXT[channel] = {
            **ctx,
            "features": all_features,
        }
        logger.info(
            f"Compare context expanded to {len(all_features)} features "
            f"for cross-group comparison"
        )

    logger.info(f"Compare prompt posted for {feature_nm}")


@app.action("heatmap_back_to_group")
def handle_heatmap_back_to_group(ack, body, client, logger):
    ack()
    value = body["actions"][0]["value"]
    parts = value.split("|")
    group_nm = parts[0]
    cloud = parts[1]
    fy = parts[2]
    channel = body["channel"]["id"]
    message_ts = body["message"]["ts"]

    ctx = HEATMAP_CONTEXT.get(channel)
    features = ctx.get("features", []) if ctx else []
    group_feats = [
        f for f in features
        if f.get("feature_group") == group_nm
    ]

    if not group_feats:
        client.chat_postMessage(
            channel=channel,
            thread_ts=message_ts,
            text=f":x: Group *{group_nm}* not found in context."
        )
        return

    worst = min(group_feats, key=lambda f: f.get("score", 100))
    portfolio, family = _CLOUD_MAPPING.get(
        cloud, ("Commerce", "B2B Commerce")
    )
    movers_data = get_feature_account_movers(
        feature_id=worst.get("feature_id"),
        snapshot_date=worst.get("data_dt") or "",
        portfolio=portfolio,
        family=family,
    )
    drilldown_blocks = build_group_drilldown_blocks(
        group_feats,
        group_nm,
        cloud,
        fy,
        movers_data,
        user_id=body["user"]["id"],
        is_on_watchlist_fn=is_on_watchlist,
    )
    client.chat_postMessage(
        channel=channel,
        thread_ts=message_ts,
        text=f":mag: {group_nm} — back to group",
        blocks=drilldown_blocks
    )
    logger.info(f"Back to group posted: {group_nm}")


@app.command("/attrition-clouds")
def attrition_clouds(ack, say):
    """Show available product clouds in Snowflake."""
    ack()
    try:
        from domain.analytics.snowflake_client import get_snowflake_connection

        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT APM_LVL_2, COUNT(*) cnt "
            "FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT "
            "WHERE SNAPSHOT_DT = (SELECT MAX(SNAPSHOT_DT) "
            "FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT) "
            "AND APM_LVL_2 IS NOT NULL "
            "GROUP BY APM_LVL_2 ORDER BY APM_LVL_2"
        )
        rows = cursor.fetchall()
        cursor.close()
        out = [f"- {r[0]} ({r[1]} accounts)" for r in rows]
        say(
            f":cloud: *Available Products ({len(rows)} total):*\n"
            + "\n".join(out)
            + "\n\n_Usage: `/attrition-risk <Account Name>`_"
        )
    except Exception as e:
        say(":x: Error: " + str(e))


@app.command("/gm-review-canvas")
def gm_review_canvas(ack, say, command, client):
    """
    Generate GM Review canvas for at-risk renewals.
    Usage: optional cloud as first comma-separated token, then accounts or opp IDs.
    """
    ack()

    text = command.get("text", "").strip()

    if not text:
        say(
            ":warning: *Usage:*\n"
            "`/gm-review-canvas <Account Names or Opp IDs>`\n"
            "`/gm-review-canvas Commerce Cloud, Acme Corp, Wayne Enterprises`\n"
            "`/gm-review-canvas B2C Commerce, Adidas AG, Oxford Industries`\n\n"
            "*Examples:*\n"
            "- `/gm-review-canvas Adidas AG, Oxford Industries`\n"
            "- `/gm-review-canvas Commerce Cloud, Adidas AG, Oxford Industries`\n"
            "- `/gm-review-canvas 006XXXXXXXXXXXX`\n\n"
            ":bulb: Tip: Cloud name is optional; it defaults to *Commerce Cloud* "
            "(or is inferred from the first *Opportunity Id* when you list only `006…` ids).\n"
            "_You can list several accounts or opp IDs; they run *one at a time* by default "
            "(set `GM_REVIEW_MAX_CONCURRENT` in `.env` only if your Snowflake pool can handle parallel bursts)._"
        )
        return

    import re as _re_gm_opp_id

    from filter_parser import CLOUD_KEYWORDS, parse_filters

    filters = parse_filters(text)
    detected_cloud = filters.get("cloud", "Commerce Cloud")
    cloud_explicit = bool(filters.get("cloud_explicit"))

    cloud_lower = {kw.lower() for kw in CLOUD_KEYWORDS}
    raw_parts = [p.strip() for p in text.split(",") if p.strip()]
    inputs = []
    for part in raw_parts:
        if part.lower() in cloud_lower:
            continue
        inputs.append(part)

    def _token_is_sf_opportunity_id(token: str) -> bool:
        return bool(
            _re_gm_opp_id.match(r"^006[A-Za-z0-9]{12,18}$", (token or "").strip())
        )

    if (
        not cloud_explicit
        and inputs
        and all(_token_is_sf_opportunity_id(p) for p in inputs)
    ):
        from domain.salesforce.org62_client import infer_cloud_from_opportunity_id

        inferred = infer_cloud_from_opportunity_id(inputs[0].strip())
        if inferred:
            detected_cloud = inferred

    if not inputs:
        say(
            ":warning: No accounts found in that command.\n"
            f"Detected cloud: *{detected_cloud}*\n"
            "Add account names or opportunity IDs after the cloud (or omit the cloud to use defaults)."
        )
        return

    say(
        f":hourglass_flowing_sand: Generating GM reviews for *{len(inputs)}* account(s) "
        f"in *{detected_cloud}*…\n"
        "_Accounts run sequentially by default — large batches may take several minutes._"
    )

    def process():
        try:
            from datetime import date as date_type

            from domain.content.canvas_builder import create_canvas
            from domain.integrations.gsheet_exporter import export_to_gsheet
            from services.gm_review_workflow import GMReviewWorkflow

            workflow = GMReviewWorkflow(
                call_llm_fn=server.call_llm_gateway_with_retry,
            )

            today_hdr = date_type.today().strftime("%A, %B %d, %Y")
            q = filters.get("quarter") or "Q2"
            fy = filters.get("fy") or "FY2027"
            filter_label = f"{detected_cloud} - {q} {fy}"

            out = workflow.run(
                inputs,
                cloud=detected_cloud,
                filter_label=filter_label,
                today=today_hdr,
            )
            reviews = out.get("reviews") or []
            combined_canvas = (out.get("combined_canvas") or "").strip()

            if not reviews or not combined_canvas:
                say(
                    ":x: No reviews generated. Check account names or IDs and try again."
                )
                return

            user_id = command.get("user_id") or ""
            title = f"{detected_cloud} GM Review — {today_hdr}"
            canvas_url = create_canvas(
                client, title=title, markdown=combined_canvas, user_id=user_id
            )

            if canvas_url:
                say(
                    f":white_check_mark: *GM Review canvas created!* {len(reviews)} account(s) | "
                    f"{detected_cloud}\n"
                    f":memo: <{canvas_url}|View Canvas>"
                )
            else:
                say(
                    text=(
                        f":white_check_mark: Generated {len(reviews)} GM review(s) "
                        f"for {detected_cloud}."
                    ),
                    blocks=[
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": combined_canvas[:3000],
                            },
                        }
                    ],
                )

            try:
                gsheet_env = os.getenv("GSHEET_ID") or os.getenv("GOOGLE_SHEET_ID")
                print(
                    f"[gm-review-canvas] Sheets export start: "
                    f"{len(reviews)} review(s), GSHEET_ID={'set' if gsheet_env else 'MISSING'}"
                )
                sheet_name = date_type.today().strftime("GM Review %Y-%m-%d")
                sheet_url = export_to_gsheet(
                    reviews, sheet_name=sheet_name, cloud=detected_cloud
                )
                if sheet_url:
                    print(f"[gm-review-canvas] Sheets export OK: {sheet_url[:80]}...")
                    say(
                        f":bar_chart: *Exported to Google Sheets!*\n"
                        f"<{sheet_url}|View Sheet>"
                    )
                elif not gsheet_env:
                    print("[gm-review-canvas] Sheets skipped: GSHEET_ID / GOOGLE_SHEET_ID not set")
                    say(
                        ":warning: Google Sheets export skipped "
                        "(`GSHEET_ID` / `GOOGLE_SHEET_ID` not set)."
                    )
                else:
                    print(
                        "[gm-review-canvas] Sheets export returned empty URL — "
                        "see gsheet_exporter logs above (often 403: share sheet with service account; "
                        "or restart the bot after adding GSHEET_ID to .env)."
                    )
                    say(
                        ":warning: Canvas created, but Google Sheets export did not return a link. "
                        "Check the bot console for `❌ Google Sheets export` / tracebacks; share the "
                        "spreadsheet with the service account from `./venv/bin/python get_sa_email.py`; "
                        "if you just added `GSHEET_ID`, restart `slack_app.py`."
                    )
            except Exception as ex_sheet:
                print(f"[gm-review-canvas] ❌ Sheets export error: {ex_sheet!r}")
                import traceback as _tb

                _tb.print_exc()
                say(
                    f":warning: Canvas posted but Google Sheets export failed: {str(ex_sheet)[:200]}"
                )

        except Exception as e:
            say(f":x: Error generating GM reviews: {str(e)}")
            print(f"GM Review error: {e}")
            import traceback

            traceback.print_exc()

    import threading

    threading.Thread(target=process).start()


def _fmt_short_money(v) -> str:
    try:
        x = float(v)
    except (TypeError, ValueError):
        s = str(v or "").strip()
        return s if s else "N/A"
    x = abs(x)
    if x >= 1_000_000:
        return f"${x / 1_000_000:.1f}M"
    if x >= 1_000:
        return f"${x / 1_000:.0f}K"
    return f"${x:,.0f}"


def _fmt_short_text(v, limit: int) -> str:
    s = " ".join(str(v or "").split())
    if not s:
        return ""
    return (s[: limit - 1] + "…") if len(s) > limit else s


@app.command("/gm-review-lists")
def gm_review_lists(ack, say, command, client):
    """
    Generate/update GM Review Slack List.
    Usage: `/gm-review-lists Commerce Cloud, Adidas AG, Oxford Industries`
    """
    ack()
    text = (command.get("text") or "").strip()
    if not text:
        say(
            ":warning: *Usage:*\n"
            "`/gm-review-lists <Account Names or Opp IDs>`\n"
            "`/gm-review-lists Commerce Cloud, Acme Corp, Wayne Enterprises`\n"
            "`/gm-review-lists 006XXXXXXXXXXXX, 006YYYYYYYYYYYY`\n\n"
            ":bulb: Tip: Cloud is optional (defaults to Commerce Cloud)."
        )
        return

    say(":hourglass_flowing_sand: Generating GM Review list…")

    def process():
        try:
            from datetime import date as date_type

            from domain.content.list_builder import update_slack_list
            from filter_parser import CLOUD_KEYWORDS, parse_filters
            from services.gm_review_bulk_workflow import run_bulk_gm_review
            from services.gm_review_workflow import GMReviewWorkflow

            filters = parse_filters(text)
            detected_cloud = filters.get("cloud", "Commerce Cloud")
            cloud_explicit = bool(filters.get("cloud_explicit"))
            bulk_mode = os.getenv("GM_REVIEW_BULK_MODE", "0") == "1"

            cloud_lower = {kw.lower() for kw in CLOUD_KEYWORDS}
            raw_parts = [p.strip() for p in text.split(",") if p.strip()]
            inputs = [p for p in raw_parts if p.lower() not in cloud_lower]

            if not inputs and not bulk_mode:
                say(
                    ":warning: No accounts found in that command.\n"
                    f"Detected cloud: *{detected_cloud}*"
                )
                return

            import re as _re_gm_opp_id

            def _token_is_sf_opportunity_id(token: str) -> bool:
                return bool(
                    _re_gm_opp_id.match(r"^006[A-Za-z0-9]{12,18}$", (token or "").strip())
                )

            if (
                not cloud_explicit
                and inputs
                and all(_token_is_sf_opportunity_id(p) for p in inputs)
            ):
                from domain.salesforce.org62_client import infer_cloud_from_opportunity_id

                inferred = infer_cloud_from_opportunity_id(inputs[0].strip())
                if inferred:
                    detected_cloud = inferred

            today_hdr = date_type.today().strftime("%A, %B %d, %Y")
            q = filters.get("quarter") or "Q2"
            fy = filters.get("fy") or "FY2027"
            filter_label = f"{detected_cloud} - {q} {fy}"

            if bulk_mode:
                reviews = run_bulk_gm_review(
                    detected_cloud,
                    fy=filters.get("fy"),
                    opp_ids=filters.get("opp_ids") or [],
                    min_attrition=filters.get("min_attrition", 500000),
                    limit=500,
                )
            else:
                workflow = GMReviewWorkflow(call_llm_fn=server.call_llm_gateway_with_retry)
                out = workflow.run(
                    inputs,
                    cloud=detected_cloud,
                    filter_label=filter_label,
                    today=today_hdr,
                )
                reviews = out.get("canvas_reviews") or []
            if not reviews:
                say(":x: No reviews generated.")
                return

            list_id = (os.getenv("GM_REVIEW_LIST_ID") or "").strip()
            if not list_id:
                say(":warning: `GM_REVIEW_LIST_ID` not set in `.env` — list not updated.")
                return

            result = update_slack_list(client, list_id, reviews)
            say(
                f":white_check_mark: *GM Review List Updated!*\n"
                f"- {result.get('updated', 0)} account(s) added\n"
                f"- Errors: {len(result.get('errors') or [])}\n"
                f"- Cloud: {detected_cloud}\n"
                f"- List ID: `{list_id}`"
            )
        except Exception as e:
            say(f":x: Error generating GM Review list: {str(e)[:200]}")
            import traceback

            traceback.print_exc()

    import threading

    threading.Thread(target=process, daemon=True).start()


@app.command("/gm-review-sheet")
def gm_review_sheet(ack, say, command, client):
    """
    Generate GM Review data and export to Google Sheets.
    Usage: `/gm-review-sheet Commerce Cloud`
    """
    ack()
    text = (command.get("text") or "").strip()
    if not text:
        say(
            ":warning: *Usage:*\n"
            "`/gm-review-sheet <Cloud or filters>`\n"
            "`/gm-review-sheet Commerce Cloud`\n"
            "`/gm-review-sheet Commerce Cloud, FY2027`\n\n"
            ":bulb: Tip: This command currently runs in bulk mode."
        )
        return

    say(":hourglass_flowing_sand: Generating GM Review sheet…")

    def process():
        try:
            from datetime import date as date_type

            from domain.integrations.gsheet_exporter import export_to_gsheet
            from filter_parser import parse_filters
            from services.gm_review_bulk_workflow import run_bulk_gm_review

            filters = parse_filters(text)
            detected_cloud = filters.get("cloud", "Commerce Cloud")
            bulk_mode = os.getenv("GM_REVIEW_BULK_MODE", "0") == "1"
            if not bulk_mode:
                say(":x: `/gm-review-sheet` requires `GM_REVIEW_BULK_MODE=1`. Set it in `.env` and restart.")
                return

            reviews = run_bulk_gm_review(
                detected_cloud,
                fy=filters.get("fy"),
                opp_ids=filters.get("opp_ids") or [],
                min_attrition=filters.get("min_attrition", 500000),
                limit=500,
            )
            if not reviews:
                say(":x: No reviews generated.")
                return

            sheet_name = date_type.today().strftime("GM Review %Y-%m-%d")
            sheet_url = export_to_gsheet(
                reviews,
                sheet_name=sheet_name,
                cloud=detected_cloud,
            )
            if not sheet_url:
                say(":warning: Sheet export completed but no URL was returned. Check bot logs.")
                return

            say(
                f":white_check_mark: *GM Review Sheet Exported!*\n"
                f"- Rows: {len(reviews)}\n"
                f"- Cloud: {detected_cloud}\n"
                f"- <{sheet_url}|View Sheet>"
            )
        except Exception as e:
            say(f":x: Error generating GM Review sheet: {str(e)[:200]}")
            import traceback

            traceback.print_exc()

    import threading

    threading.Thread(target=process, daemon=True).start()


@app.command("/at-risk-canvas")
def at_risk_canvas(ack, say, command, client):
    """
    Generate at-risk renewals canvas across ALL clouds.
    Usage: /at-risk-canvas                    → All clouds
           /at-risk-canvas Commerce Cloud     → Filter by cloud
           /at-risk-canvas B2C Commerce       → Filter by L2
           /at-risk-canvas >500k              → Filter by ARR
    """
    ack()

    text = command.get("text", "").strip()
    filter_label = text if text else "all clouds"

    say(
        f":hourglass_flowing_sand: Generating at-risk renewals canvas...\n"
        f"_Analyzing accounts matching: {filter_label}_"
    )

    def process():
        try:
            from domain.analytics.snowflake_client import get_snowflake_connection
            from filter_parser import CLOUD_KEYWORDS, parse_filters

            where_clauses = []
            text_lower = text.lower()

            if any(kw.lower() in text_lower for kw in CLOUD_KEYWORDS):
                filters = parse_filters(text)
                cloud = filters.get("cloud", "")
                if cloud:
                    cloud_safe = cloud.replace("'", "''")
                    where_clauses.append(
                        f"AND ("
                        f"atr.APM_LVL_1 LIKE '%{cloud_safe}%' OR "
                        f"atr.APM_LVL_2 LIKE '%{cloud_safe}%' OR "
                        f"atr.APM_LVL_3 LIKE '%{cloud_safe}%'"
                        f")"
                    )

            if any(
                t in text_lower for t in [">1m", ">500k", ">400k", ">200k"]
            ):
                filters = parse_filters(text)
                min_arr = filters.get("min_attrition")
                if min_arr:
                    where_clauses.append(
                        f"AND ren.RENEWAL_AMT_CONV > {min_arr}"
                    )

            where_sql = " ".join(where_clauses)

            conn = get_snowflake_connection()
            cursor = conn.cursor()

            query = f"""
                SELECT DISTINCT
                    atr.ACCOUNT_ID,
                    ren.ACCOUNT_NM,
                    atr.APM_LVL_1,
                    atr.APM_LVL_2,
                    atr.APM_LVL_3,
                    atr.ATTRITION_PROBA as SCORE,
                    atr.ATTRITION_PROBA_CATEGORY as RISK_CLASS,
                    ren.RENEWAL_OPTY_ID_18,
                    ren.RENEWAL_AMT_CONV,
                    ren.RENEWAL_ATR_CONV,
                    NULL AS RENEWAL_CLSD_DT,
                    ren.RENEWAL_STG_NM,
                    ren.ACCOUNT_18_ID
                FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT atr
                LEFT JOIN RENEWALS.WV_CI_RENEWAL_OPTY_VW ren
                    ON atr.ACCOUNT_ID = ren.ACCT_ID
                WHERE atr.SNAPSHOT_DT = (
                    SELECT MAX(SNAPSHOT_DT)
                    FROM CSS.ATTRITION_PREDICTION_ACCT_PRODUCT
                )
                AND atr.ATTRITION_PROBA_CATEGORY IN ('High', 'Medium')
                AND atr.ACCOUNT_ID IS NOT NULL
                AND ren.RENEWAL_STG_NM NOT IN (
                    'Dead Attrition', '05 Closed', 'Dead - Duplicate',
                    'Dead - No Decision', 'Dead - No Opportunity',
                    'NP - Dead Duplicate', '08 - Closed', 'Closed',
                    'Closed and referral paid', 'Loss - Off Contract',
                    'UNKNOWN', 'Courtesy'
                )
                {where_sql}
                ORDER BY atr.ATTRITION_PROBA DESC
            """

            cursor.execute(query)
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                say(f":x: No at-risk accounts found for: {filter_label}")
                return

            result = (
                f":warning: *At-Risk Renewals — {filter_label.title()}* "
                f"({len(rows)} accounts)\n\n"
            )

            for row in rows:
                (
                    account_id,
                    account_name,
                    apm_l1,
                    apm_l2,
                    apm_l3,
                    score,
                    risk_class,
                    opp_id_18,
                    renewal_amt,
                    atr_amt,
                    close_date,
                    _stage,
                    account_18_id,
                ) = row

                aid = str(account_id).strip() if account_id is not None else ""
                sf_account_url = (
                    f"https://org62.my.salesforce.com/"
                    f"{account_18_id or aid}"
                )
                sf_opp_url = (
                    f"https://org62.my.salesforce.com/{opp_id_18}"
                    if opp_id_18
                    else None
                )

                product_path = apm_l1 or ""
                if apm_l2 and apm_l2 != apm_l1:
                    product_path += f" > {apm_l2}"
                if apm_l3 and apm_l3 != apm_l2:
                    product_path += f" > {apm_l3}"

                emoji = (
                    ":red_circle:"
                    if risk_class == "High"
                    else ":large_orange_circle:"
                )
                display_name = (
                    " ".join(str(account_name or "").split()) or f"Account {aid}"
                )

                result += f"{emoji} *<{sf_account_url}|{display_name}>*\n"
                result += f"   _{product_path}_\n"
                result += f"   Score: {score:.3f} | Risk: {risk_class}"

                if renewal_amt:
                    try:
                        result += f" | ARR: ${float(renewal_amt):,.0f}"
                    except (TypeError, ValueError):
                        result += f" | ARR: {renewal_amt}"
                if atr_amt:
                    try:
                        result += f" | ATR: ${float(atr_amt):,.0f}"
                    except (TypeError, ValueError):
                        result += f" | ATR: {atr_amt}"
                if close_date:
                    result += f" | Close: {close_date}"
                if sf_opp_url:
                    result += f" | <{sf_opp_url}|View Opp>"

                result += "\n\n"

            if len(rows) > 20:
                result += f"_...and {len(rows) - 20} more accounts_\n"

            result += (
                "\n_Use `/at-risk-canvas Commerce Cloud` or "
                "`/at-risk-canvas >500k` to filter_"
            )

            say(result)

        except Exception as e:
            say(f":x: Error: {str(e)}")
            print(f"At-risk canvas error: {e}")
            import traceback

            traceback.print_exc()

    import threading

    threading.Thread(target=process).start()


@app.command("/pulse-now")
def handle_pulse_now(ack, say, command, client):
    """
    Manual trigger for daily pulse — testing and on-demand runs.
    Usage: /pulse-now
           /pulse-now C1234567890
    """
    ack()
    try:
        user_id = command.get("user_id")
        text = (command.get("text") or "").strip()

        # Optional channel override by ID; otherwise DM the requester.
        if text and text[0] in ("C", "G", "D"):
            target_channel = text
        else:
            target_channel = user_id

        say("⏳ Running pulse now...")
        run_daily_pulse(client, target_channel=target_channel)
        say("✅ Pulse completed.")
    except Exception as e:
        log_error(f"pulse-now failed: {e}")
        say(f":x: Pulse failed: {str(e)}")


@app.command("/feature-watchlist")
def handle_watchlist_command(ack, body, client):
    ack()
    user_id = body.get("user_id") or body.get("user", {}).get("id")
    if not user_id:
        return
    items = get_user_watchlist(user_id)

    if not items:
        client.chat_postMessage(
            channel=user_id,
            blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            ":eyes: Your watchlist is empty.\n"
                            "Click *👁 Watch* on any feature to start tracking it."
                        ),
                    },
                }
            ],
            text="Your watchlist is empty.",
        )
        return

    blocks = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "👁 Your Watchlist"},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"{len(items)} feature{'s' if len(items) != 1 else ''} being tracked"
                    ),
                }
            ],
        },
        {"type": "divider"},
    ]
    for item in items:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f":eyes: *{item['feature_name']}*\n"
                        f"{item['cloud']}  ·  Added {item['added_at']}"
                    ),
                },
            }
        )
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Detail"},
                        "action_id": "heatmap_feature_detail",
                        "value": (
                            f"{item['feature_id']}|{item['feature_name']}|"
                            f"{item['cloud']}|FY2027"
                        ),
                        "style": "primary",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "❌ Remove"},
                        "action_id": "remove_from_watchlist",
                        "value": (
                            f"{item['feature_id']}|{item['feature_name']}|{item['cloud']}"
                        ),
                        "style": "danger",
                    },
                ],
            }
        )
        blocks.append({"type": "divider"})

    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        "_You'll get alerted when adoption drops significantly · "
                        "Alerts configurable via `.env`_"
                    ),
                }
            ],
        }
    )

    client.chat_postMessage(
        channel=user_id,
        blocks=blocks,
        text="Your watchlist",
    )


if __name__ == "__main__":
    if not os.environ.get("SLACK_BOT_TOKEN"):
        log_error("❌ SLACK_BOT_TOKEN not found")
        sys.exit(1)
    if not os.environ.get("SLACK_APP_TOKEN"):
        log_error("❌ SLACK_APP_TOKEN not found")
        sys.exit(1)

    print("🚀 Starting Adoption Intelligence Bot", flush=True)
    init_database()
    handler = SocketModeHandler(app, os.environ["SLACK_APP_TOKEN"])
    handler.start()
