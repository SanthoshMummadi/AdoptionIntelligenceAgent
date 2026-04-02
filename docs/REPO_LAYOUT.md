# Repository layout

## Main application

| File | Purpose |
|------|---------|
| `slack_app.py` | Primary Slack Bolt app (Socket Mode): DMs, App Home dashboard, buttons, PDF upload handling. |
| `server.py` | FastMCP server: persistent brief storage + LLM Gateway client. |
| `storage/` | Local persistence (`user_briefs.pkl`, `user_last_brief.pkl`). |
| `bot_history.db` | SQLite archive of ended user sessions (conversation history). |

## Removed / obsolete

The older “GM Review / org62 / Snowflake / canvas” workflow code and related docs are no longer used by the current entrypoint (`slack_app.py`). If you still see those folders in the repo, they should be treated as legacy and safe to remove once you confirm you won’t re-enable them.
