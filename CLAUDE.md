# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Slack-native **PM Intelligence Hub** that analyzes product briefs, runs V2MoM-style reviews, and predicts attrition risk. The system combines a **FastMCP server** with **Slack Bolt** integration, using Salesforce LLM Gateway (Claude) for AI analysis.

**Requirements:** Python 3.13+ (current: 3.13.12)

**Key capabilities:**
- Product Brief & V2MoM document analysis with configurable prompt sets
- AI Council Review (GM Review) — at-risk renewal synthesis with Salesforce + Snowflake + LLM
- Attrition risk prediction via Excel-driven models and real-time Salesforce queries
- Slack Canvas generation for executive summaries
- App Home dashboard with renewal insights
- Google Drive integration (Docs/Sheets/Slides)

**Branch context:**
- **`attrition`** (current) — Stable renewal/GM Review/Snowflake bulk track, aligned with main
- **`adoption`** — Adds adoption-centric features (Feature Activation Overview, adoption heatmaps, enhanced App Home)

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

Requires live Salesforce, Snowflake, and LLM credentials. Tests automatically skip env validation (see `PRODUCT_ADOPTION_SKIP_ENV_VALIDATION`).

### Run end-to-end tests
```bash
# As script (runs all tests sequentially)
python3 tests/test_commerce_cloud_e2e.py

# Via pytest (better for single tests or debugging)
python3 -m pytest tests/test_commerce_cloud_e2e.py -v
```

Tests GM Review workflow (Section 5: WF-* tests).

### Run dynamic account tests
```bash
python3 tests/test_dynamic_accounts.py
# or
python3 -m pytest tests/test_dynamic_accounts.py -v
```

### Run single test
```bash
python3 -m pytest tests/test_commerce_cloud_e2e.py::test_name -v
```

## Architecture

### Data Flow
**GM Review** has two workflow modes — choose based on input type and scale:

**1. Bulk workflow** (`gm_review_bulk_workflow.py`) — **Snowflake-first**
- **When to use:** Large cloud scans (50+ accounts), FY filters, ATR thresholds
- **Flow:** 3 Snowflake queries → in-memory join → parallel LLM analysis
- **Speed:** Fast for bulk operations (minutes for 50+ accounts)
- **Enable:** Set `GM_REVIEW_BULK_MODE=1` in `.env`

**2. Account-by-account workflow** (`gm_review_workflow.py`) — **Salesforce-first**
- **When to use:** Specific accounts/opps, need detailed Salesforce fields, custom enrichment
- **Flow:** Per-account: Salesforce query → Snowflake enrichment → LLM analysis
- **Speed:** Slower but more detailed (seconds per account)
- **Best for:** Small sets, explicit opportunity IDs, deep field inspection

**Common GM Review pipeline** (both modes):
1. **Input:** Account names, opportunity IDs, or cloud + FY filters
2. **Salesforce query:** Fetch opportunities, dynamic fields, red account flags
3. **Snowflake enrichment:** Usage (CIDM), renewals, attrition risk, health scores
4. **LLM analysis:** Risk theme classification + playbook recommendations (with circuit breaker fallback)
5. **Canvas generation:** Slack Canvas markdown table with formatted results
6. **Optional export:** Google Sheets export for downstream analysis

### Entry points
- **`slack_app.py`** — Slack Bolt app (DMs, hub modules, file ingestion, slash commands, App Home dashboard)
- **`server.py`** — FastMCP server (brief CRUD, `query_brief`, `generate_gm_reviews`, `generate_gm_review_canvas`, `health_check`)

### Core modules
- **`services/gm_review_workflow.py`** — GM Review orchestration: Salesforce + Snowflake + LLM + canvas markdown (account-by-account)
- **`services/gm_review_bulk_workflow.py`** — Bulk GM Review: 3 queries → in-memory join → faster for large cloud scans
- **`services/app_home.py`** — App Home blocks and dashboard publishing
- **`domain/salesforce/org62_client.py`** — Salesforce authentication and query execution with concurrency limiting
- **`domain/salesforce/bulk_org62.py`** — Bulk Salesforce queries for GM Review (dynamic fields, red accounts)
- **`domain/analytics/snowflake_client.py`** — Snowflake enrichment, connection pooling, attrition queries
- **`domain/analytics/bulk_cidm.py`** — Bulk CIDM usage queries
- **`domain/analytics/bulk_renewals.py`** — Bulk Snowflake renewal queries with configurable ATR thresholds
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
  - `SNOWFLAKE_CIDM_SNAPSHOT_DT=2026-04-01`

### GM Review: Bulk vs Account-by-Account
- **Bulk workflow** (`gm_review_bulk_workflow.py`) — Snowflake-first: 3 queries, in-memory join, faster for cloud-wide scans
- **Account-by-account** (`gm_review_workflow.py`) — Salesforce-first: detailed per-account queries, better for small sets or explicit opp IDs
- **When to use bulk:** Large cloud scans (50+ accounts), FY filters, ATR thresholds
- **When to use account-by-account:** Specific accounts/opps, need detailed Salesforce fields, custom enrichment

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

### Bulk GM Review Configuration
- `GM_REVIEW_BULK_MODE=1` — Enable bulk workflow (Snowflake-first, faster for cloud scans)
- **`GM_REVIEW_FCAST_ATTRITION_THRESHOLD`** (default −500000) — **Commerce bulk Query 1** opp-level cutoff on **`RENEWAL_FCAST_ATTRITION_CONV <=`** this value ($500k+ forecast attrition when set to −500000). Applies in the INNER `WHERE` together with `BOOK_OF_BUSINESS`, opp-type clauses, **`RENEWAL_FCAST_CODE`**, **`M_A_LEGACY_RENEWAL_FLAG`**, **`ACCOUNT_NM`**, **30DE / stage** carve-out (replaces legacy 13-stage `NOT IN` list), plus **`RENEWAL_CLSD_DT`** window **SYSDATE()**‑relative (rollup fcast **`HAVING`** not used as a substitute).
- **`GM_REVIEW_RENEWAL_CLSD_FORWARD_MONTHS`** (default **24**) — Commerce Query 1 close window endpoint: **`RENEWAL_CLSD_DT <= ADD_MONTHS(DATE_TRUNC('month', SYSDATE()), N)`**, so the renewal calendar is pinned to **`SYSDATE()`** rather than **`SNOWFLAKE_RENEWAL_AS_OF_DATE`** / hardcoded **`RENEWAL_FISCAL_YEAR`** buckets.
- `GM_REVIEW_LIST_ID` — Slack List ID for `/gm-review-lists` command
- **Commerce bulk Query 1** does **not** use multi-condition **`TARGET_CLOUD LIKE`** predicates on the INNER `WHERE` (uses **`BOOK_OF_BUSINESS = 'Commerce Cloud'`** instead).
- **Non‑Commerce** bulk and **explicit `opp_ids`** paths still use **`SNOWFLAKE_RENEWAL_MIN_CLOSE_MONTH`**, dead-stage exclusions, cloud filter, FY lookahead/override as before.
- For Financial Services Cloud: uses CIDM APM_L3 filtering instead of TARGET_CLOUD patterns
- Result limit: default 500 renewals per bulk query (configurable via `limit` parameter)

### Daily Pulse Scheduler
Configured via environment:
- `PULSE_FREQUENCY=daily` (or `weekly`, `hourly`)
- `PULSE_SCHEDULE_TIME=09:00` (Asia/Kolkata timezone)
- `PULSE_CHANNEL=<channel_id>` (optional)

Started automatically on bot startup via APScheduler.

## Common Commands

### Running tests
```bash
# Run all GM Review tests
python3 tests/test_commerce_cloud_e2e.py
python3 -m pytest tests/test_commerce_cloud_e2e.py -v

# Run dynamic account tests
python3 tests/test_dynamic_accounts.py

# Run single test
python3 -m pytest tests/test_commerce_cloud_e2e.py::test_name -v
```

Tests require live Salesforce, Snowflake, and LLM credentials. Tests automatically skip env validation (via `PRODUCT_ADOPTION_SKIP_ENV_VALIDATION`).

### Slack slash commands
- `/gm-review-canvas <accounts or opp IDs>` — Generate AI Council Review canvas with optional cloud token
- `/gm-review-lists` — Update Slack Lists from GM Review bulk output (requires `GM_REVIEW_LIST_ID`)
- `/gm-review-sheet` — Export GM Review bulk results to Google Sheets (requires `GSHEET_ID` or `GOOGLE_SHEET_ID`)
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

### Troubleshooting
Common issues and solutions:
- **Slack auth.test / proxy errors** — Check corporate proxy or Socket Mode firewall rules
- **Snowflake auth failures** — Confirm key path, passphrase, role, and warehouse grants (prefer key-pair auth in production, avoid `externalbrowser`)
- **Empty GM Review rows** — Validate cloud/FY filters, snapshot dates, and `GM_REVIEW_FCAST_ATTRITION_THRESHOLD`
- **LLM circuit breaker open** — Transient LLM failures trigger fallback mode; check `LLM_CIRCUIT_THRESHOLD` and `LLM_CIRCUIT_COOLDOWN` settings
- **Snowflake timeouts** — Tune timeout env vars in `.env.example` (e.g., `SNOWFLAKE_CIDM_SNAPSHOT_DT` to pin snapshot and skip `MAX` during unstable periods)
