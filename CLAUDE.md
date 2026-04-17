# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Slack-native **PM Intelligence Hub** that analyzes product briefs, runs V2MoM-style reviews, and predicts attrition risk. The system combines a **FastMCP server** with **Slack Bolt** integration, using Salesforce LLM Gateway (Claude) for AI analysis.

**Key capabilities:**
- Product Brief & V2MoM document analysis with configurable prompt sets
- AI Council Review (GM Review) — at-risk renewal synthesis with Salesforce + Snowflake + LLM
- Attrition risk prediction via Excel-driven models and real-time Salesforce queries
- Slack Canvas generation for executive summaries
- Google Drive integration (Docs/Sheets/Slides)

## Development Setup

### Install dependencies
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Configure environment
Copy `.env.example` to `.env` and configure:
- **Slack:** `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`
- **LLM:** `LLM_GATEWAY_API_KEY`, `LLM_MODEL=claude-3-7-sonnet`
- **Salesforce:** Session-based (`SF_ACCESS_TOKEN` + `SF_INSTANCE_URL`) or username-based (`SF_USERNAME` + `SF_PASSWORD`)
- **Snowflake:** `SNOWFLAKE_USER`, `SNOWFLAKE_ACCOUNT` (+ connector secrets)
- **Tableau:** `TABLEAU_SERVER`, `TABLEAU_SITE`, `TABLEAU_TOKEN_NAME`, `TABLEAU_TOKEN_SECRET`

**Important:** For production, `server.py` validates required env vars at startup. To skip for tests/scripts: set `PRODUCT_ADOPTION_SKIP_ENV_VALIDATION=1` or run from `tests/` directory.

### Google OAuth (for Drive links)
```bash
python google_auth.py
```
Creates `google_token.pkl` for Drive access.

### Run the Slack bot
```bash
python slack_app.py
```
Runs in Socket Mode; listens for DMs, file uploads, slash commands.

### Run MCP server (standalone testing)
```bash
npx @modelcontextprotocol/inspector python server.py
```

## Testing

### Run end-to-end tests
```bash
python3 tests/test_commerce_cloud_e2e.py
```
Requires live Salesforce, Snowflake, and LLM credentials. Tests GM Review workflow (Section 5: WF-* tests).

### Run dynamic account tests
```bash
python3 tests/test_dynamic_accounts.py
```

## Architecture

### Entry points
- **`slack_app.py`** — Slack Bolt app (DMs, hub modules, file ingestion, slash commands including `/gm-review-canvas`, `/at-risk-canvas`)
- **`server.py`** — FastMCP server (brief CRUD, `query_brief`, `generate_gm_reviews`, `generate_gm_review_canvas`, `health_check`)

### Core modules
- **`services/gm_review_workflow.py`** — GM Review orchestration: Salesforce + Snowflake + LLM + canvas markdown
- **`domain/salesforce/org62_client.py`** — Salesforce authentication and query execution with concurrency limiting
- **`domain/analytics/snowflake_client.py`** — Snowflake enrichment, connection pooling, attrition queries
- **`domain/intelligence/risk_engine.py`** — Risk theme classification, playbook mapping, LLM-generated analysis with fallbacks
- **`domain/content/canvas_builder.py`** — Slack Canvas markdown generation for GM Review tables
- **`domain/integrations/gsheet_exporter.py`** — Optional Google Sheets export for GM Review data

### Persistence (treat as sensitive)
- **`storage/user_briefs.pkl`** — Per-user brief text (pickle format)
- **`storage/user_last_brief.pkl`** — Last active brief pointer
- **`bot_history.db`** — SQLite conversation archive
- **`google_token.pkl`** — OAuth token for Drive access

### Configuration
- **`config.properties`** — Hub modules and analysis prompt definitions (read by `domain/content/canvas_prompts.py`)
- **`.env`** — Runtime secrets and tunables (never commit)

## Important Implementation Details

### TLS & Certificate Handling
Internal Salesforce LLM Gateway URLs use corporate PKI (not in certifi). Two approaches:
1. **Recommended:** Set `LLM_GATEWAY_CA_BUNDLE=/path/to/ca-bundle.crt`
2. **Fallback:** If no bundle specified for internal hosts, verify is automatically disabled

For non-internal URLs, TLS always uses certifi or custom CA bundle.

### Salesforce Session Management
`org62_client` refreshes sessions via `sf org display` and updates **`os.environ` only** — it does **not** rewrite `.env` on disk.

### Snowflake Service Account (Production)
- **User:** `SVC_SSE_DM_CSG_RPT_PRD_ADOPTIONBOT_PRD`
- **Auth:** Key-pair authentication (`rsa_key.p8` in `keys/` folder)
- **NEVER use `externalbrowser` authenticator** — production uses service account key-pair only
- **Snapshot dates:**
  - `SNOWFLAKE_RENEWAL_AS_OF_DATE=2026-03-01`
  - `SNOWFLAKE_CIDM_SNAPSHOT_DT=2026-04-01`

### GM Review Performance
- Default: **1 concurrent account** (gentle on Snowflake)
- Max: **12 concurrent accounts**
- Configure via `GM_REVIEW_MAX_CONCURRENT` in `.env`
- Snowflake pool size should be: `pool_size >= GM_REVIEW_MAX_CONCURRENT × 4`
- Default pool size: 16 (supports up to 4 concurrent accounts comfortably)

### LLM Circuit Breaker
Protects against LLM Gateway failures:
- Opens after `LLM_CIRCUIT_THRESHOLD` failures (default: 3)
- Stays open for `LLM_CIRCUIT_COOLDOWN` seconds (default: 300)
- When open, `risk_engine.py` uses deterministic fallback text from rules + playbooks

### Snowflake Timeout Tuning
Key timeout environment variables (see `.env.example`):
- `SNOWFLAKE_STATEMENT_TIMEOUT=30` — default query timeout
- `SNOWFLAKE_HEALTH_STATEMENT_TIMEOUT=90` — health queries
- `SNOWFLAKE_USAGE_CIDM_TIMEOUT=45` — CIDM usage queries
- `SNOWFLAKE_ATTRITION_STATEMENT_TIMEOUT=90` — attrition queries
- `SNOWFLAKE_CIDM_SNAPSHOT_DT=YYYY-MM-DD` — pin snapshot date to skip MAX during unstable runs

### Daily Pulse Scheduler
Configured via environment:
- `PULSE_FREQUENCY=daily` (or `weekly`, `hourly`)
- `PULSE_SCHEDULE_TIME=09:00` (Asia/Kolkata timezone)
- `PULSE_CHANNEL=<channel_id>` (optional)

Started automatically on bot startup via APScheduler.

## Common Commands

### Slack slash commands
- `/gm-review-canvas <accounts or opp IDs>` — Generate AI Council Review canvas with optional cloud token
- `/at-risk-canvas <optional filters>` — Generate at-risk renewal canvas
- `/attrition-risk <Account Name>` — Get attrition risk summary from Excel model
- `/attrition-clouds` — List available cloud filters
- `/risk-mapping <optional theme>` — Show risk theme to playbook mappings
- `/tableau-test` — Test Tableau integration

### Module switching in DM
- "switch to product brief" or "product brief analysis"
- "switch to v2mom" or "v2mom analysis"
- "switch to attrition risk" or "attrition risk predictor"

## Documentation

- **`docs/ARCHITECTURE.md`** — System architecture and component roles
- **`docs/ARCHITECTURE_DIAGRAM.md`** — Mermaid + ASCII diagrams
- **`docs/AI_COUNCIL_GM_REVIEW.md`** — AI Council Review deep dive (flow, AI behavior, entry points)
- **`docs/REPO_LAYOUT.md`** — File index and entrypoint catalog

## Development Notes

### Scripts directory
`scripts/` contains ad-hoc debugging tools (`debug_*.py`). These are **not maintained as product code** — if a script is not imported by `slack_app.py` or `server.py`, treat it as optional tooling.

### Experimental features
Files with `explore*.py` or `fix_*.py` prefixes are experimental. Refer to documentation for the supported product path.

### Security considerations
- Pickle files (`*.pkl`) should be treated as sensitive
- Never commit `.env` or credential files
- Corporate CA certificate handling required for internal Salesforce endpoints

### Python version
Requires Python 3.13+ (current: 3.13.12)
