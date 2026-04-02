# Adoption Intelligence Bot — Architecture

Last updated: 2026-04-02

This repo currently runs a **Slack-native product brief analysis bot** with:

- `slack_app.py`: Slack Bolt (Socket Mode) app handling DMs, App Home dashboard, and button actions.
- `server.py`: FastMCP tool server that also provides **persistent brief storage** and an **LLM Gateway client**.

For a diagram-only view, see `docs/ARCHITECTURE_DIAGRAM.md`.

## Current system overview

```text
Slack (DMs + App Home + buttons)
  → slack_app.py
      - persists: storage/user_last_brief.pkl
      - archives sessions: bot_history.db
  → server.py
      - persists: storage/user_briefs.pkl
      - calls: Salesforce LLM Gateway (/v1/chat/completions)
```

## Persistence

- **Brief storage**: `storage/user_briefs.pkl` (pickle)
- **Last active brief**: `storage/user_last_brief.pkl` (pickle)
- **Conversation session archive**: `bot_history.db` (SQLite)

## Notes

- Legacy “GM Review / org62 / Snowflake / canvas” workflow modules are no longer part of the current entrypoint and should be treated as **obsolete** unless you explicitly re-enable them.

