# Adoption Intelligence Bot

Slack-native **PM intelligence hub**: product briefs and V2MoM analysis, **AI Council / GM Review** (at‑risk renewals via Salesforce + Snowflake + Claude), **attrition risk** tooling, optional Google Sheets export, and an App Home dashboard.

---

## Branch: `attrition`

Check out **`attrition`** when you care about the **renewal / GM Review / Snowflake bulk** track and stable alignment with **`main`** (same tip as upstream `origin/main`-style lineage in this repo).

The **`adoption`** branch adds **adoption‑centric** UX and data work (for example Feature Activation Overview, adoption heatmaps, and related Slack home improvements). If you need those features, use **`adoption`** instead of **`attrition`**.

---

## What this branch delivers

| Area | What you get |
|------|----------------|
| **Slack bot** | Socket Mode: DMs, file uploads, slash commands, App Home |
| **GM Review** | Sequential (`gm_review_workflow`) and **bulk Snowflake‑first** (`gm_review_bulk_workflow`) paths; canvas markdown + optional Sheets |
| **Attrition** | `/attrition-risk`, `/attrition-clouds`, `/risk-mapping`, `/at-risk-canvas` |
| **MCP** | `server.py` — FastMCP tools for brief/query and GM Review–related operations |
| **Integrations** | Salesforce Org62, Snowflake (CIDM / renewals / attrition‑style enrichment), Salesforce LLM Gateway, optional Tableau & Google Drive |

---

## Prerequisites

- **Python 3.13+**
- Slack app: **Bot token** + **App‑level token** (Socket Mode)
- Salesforce session or username/password (see `.env.example`)
- Snowflake: service account preferred — **key‑pair auth** in production paths (avoid `externalbrowser` in prod)
- Optional: Google OAuth for Drive/Sheets (`python google_auth.py` → `google_token.pkl`)

---

## Setup

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Copy **`.env.example`** → **`.env`** and fill in secrets (never commit `.env`). Notable variables:

**Slack & LLM**

- `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`
- `LLM_GATEWAY_API_KEY`, `LLM_MODEL`

**Snowflake**

- `SNOWFLAKE_USER`, `SNOWFLAKE_ACCOUNT`, role/warehouse/database/schema
- `SNOWFLAKE_PRIVATE_KEY_PATH` (and passphrase if applicable)
- Snapshot / tuning (examples): `SNOWFLAKE_CIDM_SNAPSHOT_DT`, timeouts in `.env.example`; Commerce bulk renewals window uses **`SYSDATE()`** (see **`GM_REVIEW_RENEWAL_CLSD_FORWARD_MONTHS`** in `.env.example`)

**Salesforce**

- Session: `SF_ACCESS_TOKEN`, `SF_INSTANCE_URL` — or username/password equivalents

**Bulk GM Review tuning**

- `GM_REVIEW_BULK_MODE=1`
- `GM_REVIEW_FCAST_ATTRITION_THRESHOLD=-500000` (and related bulk filters — see `.env.example`)
- `GM_REVIEW_MAX_CONCURRENT`, list/sheet helpers: `GM_REVIEW_LIST_ID`, etc.

Internal LLM Gateway hosts may require a CA bundle (`LLM_GATEWAY_CA_BUNDLE`); see `CLAUDE.md` or `.env.example` for TLS notes.

Startup validates required env in `server.py` unless you set `PRODUCT_ADOPTION_SKIP_ENV_VALIDATION=1` or run tests from `tests/`.

---

## Run

**Slack app**

```bash
python slack_app.py
```

**MCP server (inspector)**

```bash
npx @modelcontextprotocol/inspector python server.py
```

---

## Slack slash commands (high level)

| Command | Purpose |
|---------|---------|
| `/gm-review-canvas` | AI Council / GM Review canvas from accounts or opportunity IDs |
| `/gm-review-lists` | Update Slack Lists from GM Review bulk output |
| `/gm-review-sheet` | Export GM Review bulk results to Google Sheets |
| `/at-risk-canvas` | At‑risk renewal canvas (filters optional) |
| `/attrition-risk` | Attrition summary (Excel‑backed model path) |
| `/attrition-clouds` | List cloud filters for attrition |
| `/risk-mapping` | Risk theme ↔ playbook mappings |
| `/tableau-test` | Tableau connectivity smoke test |

DM “hub” wording can switch modes (e.g. product brief, V2MoM, attrition predictor) — see `config.properties` and `CLAUDE.md`.

---

## Tests

Requires live Salesforce, Snowflake, and LLM where noted:

```bash
python3 tests/test_commerce_cloud_e2e.py
python3 tests/test_dynamic_accounts.py
python3 -m pytest tests/test_commerce_cloud_e2e.py::test_name -v
```

---

## Project layout

| Path | Role |
|------|------|
| `slack_app.py` | Bolt app entrypoint |
| `server.py` | FastMCP server |
| `services/gm_review_bulk_workflow.py` | Bulk GM Review orchestration |
| `services/gm_review_workflow.py` | Account‑by‑account GM Review |
| `services/app_home.py` | App Home blocks & publishing |
| `domain/analytics/bulk_renewals.py`, `bulk_cidm.py` | Bulk Snowflake renewals / CIDM |
| `domain/salesforce/bulk_org62.py` | Bulk Salesforce reads |
| `domain/intelligence/risk_engine.py` | Risk themes + LLM / fallbacks |
| `domain/content/canvas_builder.py` | Slack canvas markdown |
| `domain/integrations/gsheet_exporter.py` | Sheets export |
| `config.properties` | Hub modules & prompts |

---

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/ARCHITECTURE_DIAGRAM.md`](docs/ARCHITECTURE_DIAGRAM.md)
- [`docs/AI_COUNCIL_GM_REVIEW.md`](docs/AI_COUNCIL_GM_REVIEW.md)
- [`docs/REPO_LAYOUT.md`](docs/REPO_LAYOUT.md)

Agent / contributor notes live in **`CLAUDE.md`** (timeouts, circuit breaker, Daily Pulse scheduler, concurrency).

---

## Security & data

- Treat **`storage/*.pkl`**, **`bot_history.db`**, **`google_token.pkl`**, and anything under **`keys/`** as sensitive.
- Do not commit `.env`, API keys, or private keys.

---

## Troubleshooting

- **Slack `auth.test` / proxy errors** — check corporate proxy or Socket Mode firewall rules.
- **Snowflake auth** — confirm key path, passphrase, role, and warehouse grants.
- **Empty GM Review rows** — validate cloud/FY filters, snapshot dates, and `GM_REVIEW_FCAST_ATTRITION_THRESHOLD`.
