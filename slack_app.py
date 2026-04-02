from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
import os
import re
import sys
import time
import io
import json
import pickle
import sqlite3
from datetime import datetime, timezone

import PyPDF2
import requests
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _ROOT)
load_dotenv(os.path.join(_ROOT, ".env"))
import server
from log_utils import log_error

slack_bot_token = os.environ.get("SLACK_BOT_TOKEN")
if not slack_bot_token:
    raise RuntimeError(
        "Missing SLACK_BOT_TOKEN. Set it in your shell or add it to .env as:\n"
        "SLACK_BOT_TOKEN=xoxb-...\n"
    )

app = App(token=slack_bot_token)

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


# -------------------------
# Helpers
# -------------------------
def download_and_process_pdf(url: str, token: str) -> tuple[str, int]:
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, timeout=60)
    if response.status_code != 200:
        raise Exception(f"Failed to download file: {response.status_code}")

    pdf_file = io.BytesIO(response.content)
    reader = PyPDF2.PdfReader(pdf_file)
    text = ""
    for page in reader.pages:
        text += (page.extract_text() or "") + "\n"

    return text.strip(), len(reader.pages)


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
# Slack events / actions
# -------------------------
@app.event("file_shared")
def handle_file_upload(event, say, client):
    try:
        file_id = event["file_id"]
        user_id = event["user_id"]

        file_info = client.files_info(file=file_id)
        file_data = file_info["file"]
        file_name = file_data["name"]
        file_type = file_data.get("mimetype", "")

        if "pdf" not in file_type.lower() and not file_name.lower().endswith(".pdf"):
            say("I can only process PDF files right now. Please upload a PDF product brief.")
            return

        say(f"📄 Processing *{file_name}*... :hourglass:")

        file_url = file_data["url_private"]
        text_content, num_pages = download_and_process_pdf(
            file_url, os.environ.get("SLACK_BOT_TOKEN")
        )

        brief_name = file_name.replace(".pdf", "").replace(" ", "_")
        server.upload_brief_text(brief_name, text_content, user_id=user_id)

        user_last_brief[user_id] = brief_name
        save_last_briefs()

        # Update session snapshot
        s = get_session(user_id)
        s["current_brief"] = brief_name
        s["brief_count"] = len(server.get_user_briefs(user_id))

        say(
            f"✅ Got it! I've analyzed *{brief_name}* ({num_pages} pages, {len(text_content):,} characters)\n\n"
            f"*Ask me anything!* Just type naturally - no commands needed."
        )

    except Exception as e:
        say(f"❌ Error processing PDF: {str(e)}")


@app.event("message")
def handle_message(event, say, client):
    if event.get("bot_id") or event.get("subtype"):
        return

    text = event.get("text", "").strip()
    user = event["user"]

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
    user_id = event["user"]

    if event.get("tab") == "messages":
        return

    try:
        user_info = client.users_info(user=user_id)
        first_name = user_info["user"]["profile"].get("first_name", "there")
    except Exception:
        first_name = "there"

    briefs = server.get_user_briefs(user_id)
    sess = active_sessions.get(user_id) or {}
    has_active_session = bool(sess.get("messages"))

    # Hub: no briefs yet, or no active DM session (e.g. after clear)
    show_hub = (not briefs) or (not has_active_session)

    try:
        if show_hub:
            from agent import build_home_view

            view = build_home_view(user_id, first_name)
            client.views_publish(user_id=user_id, view=view)
        else:
            active_brief = user_last_brief.get(user_id, list(briefs.keys())[0])

            hour = datetime.now().hour
            greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 17 else "Good evening"

            blocks = [
                {"type": "header", "text": {"type": "plain_text", "text": f"{greeting}, {first_name} 👋"}},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"I'm tracking *{len(briefs)} product brief(s)* for you.\n\n"
                            f"*Currently analyzing:* {active_brief}"
                        ),
                    },
                },
                {"type": "divider"},
                {"type": "section", "text": {"type": "mrkdwn", "text": "*What do you want to explore?*"}},
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "📊 Adoption Risks", "emoji": True},
                            "action_id": "quick_adoption_risks",
                            "value": active_brief,
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "🎯 Big Rocks", "emoji": True},
                            "action_id": "quick_big_rocks",
                            "value": active_brief,
                        },
                    ],
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "📈 Success Metrics", "emoji": True},
                            "action_id": "quick_metrics",
                            "value": active_brief,
                        },
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "👥 Target Audience", "emoji": True},
                            "action_id": "quick_audience",
                            "value": active_brief,
                        },
                    ],
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {"type": "plain_text", "text": "📝 Full Summary", "emoji": True},
                            "action_id": "quick_summary",
                            "value": active_brief,
                            "style": "primary",
                        },
                    ],
                },
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Your Briefs ({len(briefs)}):*"},
                },
            ]

            if len(briefs) > 1:
                brief_buttons = []
                for brief_name in list(briefs.keys())[:5]:
                    is_active = brief_name == active_brief
                    label = f"{'✓ ' if is_active else ''}{brief_name}"
                    if len(label) > 75:
                        label = label[:72] + "..."
                    btn = {
                        "type": "button",
                        "text": {"type": "plain_text", "text": label, "emoji": True},
                        "action_id": f"switch_to_{brief_name}",
                        "value": brief_name,
                    }
                    if is_active:
                        btn["style"] = "primary"
                    brief_buttons.append(btn)
                blocks.append({"type": "actions", "elements": brief_buttons})
            else:
                blocks.append({
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": f"• {active_brief}"}],
                })

            blocks.extend(
                [
                    {"type": "divider"},
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "📎 Upload New Brief", "emoji": True},
                                "action_id": "upload_new_brief",
                            },
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "⛔ End Session", "emoji": True},
                                "action_id": "btn_end_session_home",
                                "style": "danger",
                            },
                        ],
                    },
                ]
            )

            client.views_publish(user_id=user_id, view={"type": "home", "blocks": blocks})

    except SlackApiError as e:
        err = e.response.get("error") if getattr(e, "response", None) else None
        if err == "not_enabled":
            msg = (
                "App Home is not available for this app (views.publish: not_enabled). "
                "In api.slack.com: App → App Home → enable a Home tab, Bot Token Scopes → add "
                "`app_home:write`, then reinstall the app to the workspace."
            )
            if logger:
                logger.warning(msg)
            else:
                print(f"⚠️  {msg}")
        else:
            if logger:
                logger.error(f"Error publishing home tab: {e}")
            else:
                print(f"Error publishing home tab: {e}")
    except Exception as e:
        if logger:
            logger.error(f"Error publishing home tab: {e}")
        else:
            print(f"Error publishing home tab: {e}")


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


@app.action("module_product_brief")
def handle_module_product_brief(ack, body, client):
    """Handle Product Brief Analysis module selection."""
    ack()
    user_id = body["user"]["id"]
    client.chat_postMessage(
        channel=user_id,
        text=(
            ":brain: *Product Brief Analysis Module*\n\n"
            "Upload a product brief PDF and ask me things like:\n"
            "- What are the key adoption risks?\n"
            "- Summarize the success metrics\n"
            "- What are the big rocks for this quarter?\n"
            "- Who is the target audience?\n\n"
            "Just send me a PDF to get started!"
        ),
    )


@app.action("module_v2mom")
def handle_module_v2mom(ack, body, client):
    """Handle V2MoM Analysis module selection."""
    ack()
    user_id = body["user"]["id"]
    client.chat_postMessage(
        channel=user_id,
        text=(
            ":dart: *V2MoM Analysis Module*\n\n"
            "I'll help align your product brief with V2MoM framework:\n"
            "- Vision alignment\n"
            "- Value proposition mapping\n"
            "- Obstacle identification\n"
            "- Method validation\n\n"
            "Upload a brief and ask: _'How does this align with our V2MoM?'_"
        ),
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
    from domain.salesforce.org62_client import get_sf_client
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

    sf = get_sf_client()
    id_to_name: dict[str, str] = {}
    ids = list({r["account_id"] for r in records})
    try:
        for i in range(0, len(ids), 50):
            batch = ids[i : i + 50]
            id_list = "','".join(str(b) for b in batch)
            result = sf.query(
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
                enrich_account,
                format_enrichment_for_display,
                get_account_attrition,
            )
            from domain.content.canvas_builder import build_account_brief_blocks
            from domain.intelligence.risk_engine import generate_risk_analysis
            from domain.salesforce.org62_client import (
                _escape,
                _soql_line,
                get_red_account,
                get_renewal_opportunities,
                get_renewal_opportunities_any_cloud,
                get_sf_client,
                resolve_account_enhanced,
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

                sf = get_sf_client()
                try:
                    result = sf.query(
                        _soql_line(
                            f"""
                        SELECT
                            Id, Name, StageName, Amount, CloseDate,
                            Account.Id, Account.Name,
                            Account.BillingCountry,
                            ForecastCategoryName,
                            Forecasted_Attrition__c, Swing__c,
                            License_At_Risk_Reason__c,
                            ACV_Reason_Detail__c, NextStep,
                            Description, Specialist_Sales_Notes__c,
                            Manager_Forecast_Judgement__c
                        FROM Opportunity
                        WHERE Id = '{_escape(opp_id)}'
                        LIMIT 1
                    """
                        )
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
                fut_enrich = ex.submit(enrich_account, account_id, opty_id, detected_cloud)
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
                call_llm_fn=server.call_llm_gateway,
            )

            health_for_tldr = display.get("health_display") or display.get(
                "ari_category", "N/A"
            )
            try:
                tldr = server.call_llm_gateway(
                    "Summarize in 2 sentences for a PM: "
                    + "Account: "
                    + account_name
                    + " | ARI: "
                    + str(display.get("ari_category", "N/A"))
                    + " | ATR: "
                    + str(
                        abs(opp.get("Forecasted_Attrition__c") or 0) if opp else 0
                    )
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
            )
            say(
                text="Account Risk Briefing — " + account_name,
                blocks=blocks,
            )

        threading.Thread(target=process).start()

    except Exception as e:
        say(f"❌ Error: {str(e)}")


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
            ":bulb: Tip: Cloud name is optional; it defaults to *Commerce Cloud*."
        )
        return

    from filter_parser import CLOUD_KEYWORDS, parse_filters

    filters = parse_filters(text)
    detected_cloud = filters.get("cloud", "Commerce Cloud")

    cloud_lower = {kw.lower() for kw in CLOUD_KEYWORDS}
    raw_parts = [p.strip() for p in text.split(",") if p.strip()]
    inputs = []
    for part in raw_parts:
        if part.lower() in cloud_lower:
            continue
        inputs.append(part)

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
        "_This may take 30–60 seconds._"
    )

    def process():
        try:
            from datetime import date as date_type

            from domain.content.canvas_builder import create_canvas
            from domain.integrations.gsheet_exporter import export_to_gsheet
            from services.gm_review_workflow import GMReviewWorkflow

            workflow = GMReviewWorkflow(
                call_llm_fn=server.call_llm_gateway,
                max_concurrent=5,
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
                sheet_url = export_to_gsheet(reviews, sheet_name=sheet_name)
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
                    ren.RENEWAL_CLSD_DT,
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
                {where_sql}
                ORDER BY atr.ATTRITION_PROBA DESC
                LIMIT 50
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

            for row in rows[:20]:
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
