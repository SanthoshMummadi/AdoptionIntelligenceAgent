# Repository layout

## Application entrypoints

| File | Purpose |
|------|---------|
| `slack_app.py` | Slack Bolt app (Socket Mode): DMs, hub modules, file ingest, slash commands including **`/gm-review-canvas`**, **`/at-risk-canvas`**, attrition helpers. Imports **`server`** for LLM and shared config. |
| `server.py` | FastMCP server: brief tools, **`generate_gm_reviews`** / **`generate_gm_review_canvas`**, `health_check`, LLM Gateway client (TLS + circuit breaker). Startup env validation (with test/script skips). |
| `agent.py` | Agent routing helper; may reference GM Review intents. |

## Core packages

| Area | Path | Notes |
|------|------|--------|
| GM Review / AI Council orchestration | `services/gm_review_workflow.py` | `GMReviewWorkflow` — SF + Snowflake + `risk_engine` + canvas markdown. |
| Salesforce | `domain/salesforce/org62_client.py` | Shared by Slack, server, workflow. |
| Snowflake | `domain/analytics/snowflake_client.py` | Enrichment, attrition, connection pool. |
| Intelligence | `domain/intelligence/risk_engine.py` | Themes, playbooks, LLM prompts for risk notes / recommendations. |
| Canvas / markdown | `domain/content/canvas_builder.py` | GM Review tables, adoption POV, Slack canvas content. |
| Google Sheets | `domain/integrations/gsheet_exporter.py` | Optional export after GM Review canvas. |

## Persistence

| Location | Purpose |
|----------|---------|
| `storage/user_briefs.pkl` | MCP brief storage |
| `storage/user_last_brief.pkl` | Slack “last brief” pointer |
| `bot_history.db` | SQLite session archive |

## Configuration

| File | Purpose |
|------|---------|
| `.env` / `.env.example` | Secrets and tunables (never commit `.env`) |
| `config.properties` | Hub modules and prompts (`canvas_prompts.py`) |

## Documentation

| File | Purpose |
|------|---------|
| `docs/ARCHITECTURE.md` | Narrative architecture |
| `docs/ARCHITECTURE_DIAGRAM.md` | Mermaid + ASCII diagrams |
| `docs/AI_COUNCIL_GM_REVIEW.md` | AI Council Review (= GM Review) — flow, modules, AI behavior |

## Tests & scripts

| Path | Purpose |
|------|---------|
| `tests/test_commerce_cloud_e2e.py` | Live e2e: Salesforce, Snowflake, GM Review sections (run: `python3 tests/test_commerce_cloud_e2e.py`) |
| `scripts/` | Debugging and one-off exports; not required for production path |

## Experimental / legacy

Files under `scripts/debug_*.py` and similar are **ad-hoc**. Prefer **`docs/REPO_LAYOUT.md`** + **`docs/ARCHITECTURE.md`** for the supported product path. If a script is not imported by `slack_app.py` or `server.py`, treat it as optional tooling.
