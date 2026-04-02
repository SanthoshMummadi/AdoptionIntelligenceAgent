# Adoption Intelligence Bot — Architecture Diagram

This file is the lightweight “diagram-only” companion to `docs/ARCHITECTURE.md`.

It reflects the **current runnable path** in this repo: `slack_app.py` (Slack Bot) + `server.py` (FastMCP + brief storage + LLM Gateway client).

## ASCII overview (current)

```text
Slack (DMs + App Home + buttons)
            │
            ▼
       slack_app.py
            │
            ├── reads/writes: storage/user_last_brief.pkl
            ├── writes:      bot_history.db   (SQLite session archive)
            │
            ▼
         server.py
            │
            ├── reads/writes: storage/user_briefs.pkl  (per-user briefs)
            │
            └── calls: Salesforce LLM Gateway (chat/completions)
```

## Mermaid diagram (current)

```mermaid
flowchart TB
  Slack[Slack<br/>DMs + App Home + Buttons] --> Bot[slack_app.py<br/>Slack Bolt (Socket Mode)]

  Bot -->|upload PDF + user messages| Srv[server.py<br/>FastMCP tools + brief store + LLM client]
  Bot -->|persist last brief| Last[(storage/user_last_brief.pkl)]
  Bot -->|archive sessions| DB[(bot_history.db)]

  Srv -->|persist briefs| Briefs[(storage/user_briefs.pkl)]
  Srv -->|HTTPS| LLM[Salesforce LLM Gateway<br/>/v1/chat/completions]
```

## Obsolete/legacy components (not used by current entrypoint)

The following modules exist in the repo but are **not imported by** `slack_app.py` today. If you’re simplifying the repo, consider moving these into `legacy/` or deleting after you confirm they’re not needed:

- `adapters/` (e.g. `adapters/slack_adapter_parallel.py`, `adapters/mcp_adapter.py`)
- `services/` (e.g. `services/gm_review_workflow.py`)
- `domain/` (e.g. `domain/salesforce/org62_client.py`, `domain/analytics/snowflake_client.py`, `domain/content/canvas_builder.py`)

