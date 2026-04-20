# Adoption Intelligence Bot

Slack-native PM intelligence hub for:
- Product brief and V2MoM analysis
- GM Review (at-risk renewal workflows)
- Attrition risk analysis and exports
- App Home operational dashboard

## Core Features

- **Slack Bot + Socket Mode**
  - DM interactions, slash commands, file ingestion, App Home publishing
- **Bulk GM Review pipeline**
  - Snowflake-first bulk renewals + CIDM enrichment + Salesforce dynamic fields
  - `/gm-review-lists` for list updates
  - `/gm-review-sheet` for Google Sheets export
- **App Home dashboard**
  - At-risk counts for Commerce + FSC
  - Refresh action and quick GM Review buttons
  - Top red accounts summary
- **Attrition tooling**
  - `/attrition-risk`, `/attrition-clouds`, `/risk-mapping`, `/at-risk-canvas`
- **Config-driven prompts**
  - Hub/module prompts from `config.properties`

## Project Structure

- `slack_app.py` - main Slack Bolt entrypoint
- `server.py` - FastMCP server entrypoint
- `services/gm_review_bulk_workflow.py` - bulk GM Review orchestration
- `services/gm_review_workflow.py` - legacy/per-account GM Review flow
- `services/app_home.py` - App Home dashboard blocks + publishing
- `domain/analytics/bulk_renewals.py` - bulk renewals query and filters
- `domain/analytics/bulk_cidm.py` - bulk CIDM usage/enrichment
- `domain/salesforce/bulk_org62.py` - bulk Salesforce opportunity fields
- `domain/integrations/gsheet_exporter.py` - sheet export adapter/writer
- `domain/content/list_builder.py` - Slack List field mapping/update
- `domain/analytics/snowflake_client.py` - Snowflake connection/query utilities
- `docs/ARCHITECTURE.md` - architecture details
- `docs/AI_COUNCIL_GM_REVIEW.md` - GM Review flow deep-dive

## Prerequisites

- Python 3.13+
- Slack app credentials (bot + app tokens)
- Salesforce access (session token or username/password flow)
- Snowflake service account + key pair
- Optional: Google OAuth token for Drive/Sheets integration

## Local Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create `.env` (do not commit it) and set required keys such as:
- `SLACK_BOT_TOKEN`
- `SLACK_APP_TOKEN`
- `LLM_GATEWAY_API_KEY`
- `LLM_MODEL`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_SCHEMA`
- `SNOWFLAKE_PRIVATE_KEY_PATH`
- `SF_ACCESS_TOKEN`
- `SF_INSTANCE_URL`

Common runtime flags used by this project:
- `GM_REVIEW_BULK_MODE=1`
- `GM_REVIEW_LIST_ID=<slack_list_id>`
- `GM_REVIEW_FCAST_ATTRITION_THRESHOLD=-500000`
- `SNOWFLAKE_CIDM_SNAPSHOT_DT=YYYY-MM-DD`
- `SNOWFLAKE_RENEWAL_AS_OF_DATE=YYYY-MM-DD`
- `SNOWFLAKE_CSS_SKIP=1`

## Run

```bash
./venv/bin/python slack_app.py
```

For MCP server testing:

```bash
npx @modelcontextprotocol/inspector python server.py
```

## Slack Commands

- `/gm-review-lists <cloud or filters>`
- `/gm-review-sheet <cloud or filters>`
- `/gm-review-canvas <accounts/opps>` (stub/deprecated path depending on config)
- `/at-risk-canvas <optional filters>`
- `/attrition-risk <Account Name>`
- `/attrition-clouds`
- `/risk-mapping <optional theme>`
- `/tableau-test`

## App Home

App Home is published on `app_home_opened` and supports:
- `refresh_app_home`
- `run_gm_review_commerce`
- `run_gm_review_fsc`

Implementation: `services/app_home.py` and related handlers in `slack_app.py`.

## Data & Security Notes

- Treat `storage/*.pkl`, `bot_history.db`, and OAuth tokens as sensitive.
- Never commit `.env`, keys, or credentials.
- Keep `keys/` ignored in git.
- Snowflake production path uses key-pair auth (no `externalbrowser` in prod).

## Troubleshooting

- **Slack auth/proxy errors**
  - If startup fails at `auth.test` with proxy tunnel/403, check local network/proxy rules.
- **Snowflake connectivity**
  - Verify key path/passphrase and warehouse permissions.
- **No GM Review rows**
  - Validate cloud filters, fiscal filters, and attrition threshold in `.env`.

## Documentation

- `docs/ARCHITECTURE.md`
- `docs/ARCHITECTURE_DIAGRAM.md`
- `docs/AI_COUNCIL_GM_REVIEW.md`
- `docs/REPO_LAYOUT.md`