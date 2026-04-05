# Adoption Intelligence — Architecture

Last updated: 2026-04-02

This repository delivers a **Slack-native PM Intelligence Hub** and a **FastMCP** server that share Python modules for brief analysis, attrition workflows, and **GM Review / AI Council Review** (at-risk renewal synthesis).

- **Diagrams (Mermaid):** `docs/ARCHITECTURE_DIAGRAM.md`
- **AI Council / GM Review (deep dive):** `docs/AI_COUNCIL_GM_REVIEW.md`
- **File index:** `docs/REPO_LAYOUT.md`

---

## 1. Runtime overview

```text
Slack (DM, slash commands, files, Canvas)
  → slack_app.py  (Bolt, Socket Mode)
      → import server  (shared LLM + optional MCP tools in same process)
      → persistence: user_last_brief.pkl, bot_history.db, …

MCP clients / Inspector
  → server.py  (FastMCP)
      → storage/user_briefs.pkl
      → Salesforce LLM Gateway (TLS: certifi or LLM_GATEWAY_CA_BUNDLE)

Both paths may call:
  → domain/salesforce/org62_client.py
  → domain/analytics/snowflake_client.py
  → services/gm_review_workflow.py  (GM Review / AI Council batch flow)
```

---

## 2. Major components

| Component | Role |
|-----------|------|
| **`slack_app.py`** | User-facing app: hub modules, brief upload, attrition commands, **`/gm-review-canvas`**, session + SQLite history. |
| **`server.py`** | FastMCP tool host: brief CRUD, `query_brief`, **`generate_gm_reviews`**, **`generate_gm_review_canvas`**, `health_check`, LLM client + circuit breaker. |
| **`services/gm_review_workflow.py`** | Orchestrates per-account SF + Snowflake + LLM + canvas markdown; emits structured logs (`account_review`, timings, `run_id`). |
| **`domain/salesforce/org62_client.py`** | Salesforce auth and `sf_query`; concurrency limiter; structured errors for limits/timeouts. |
| **`domain/analytics/snowflake_client.py`** | Enrichment pool, queries, slow-query logging. |
| **`domain/intelligence/risk_engine.py`** | Risk theme classification, playbook map, **`generate_risk_analysis`** (LLM + fallbacks). |
| **`domain/content/canvas_builder.py`** | GM Review table markdown, adoption POV, Slack-oriented formatting. |
| **`domain/integrations/gsheet_exporter.py`** | Optional GM Review export to Google Sheets (Slack command path). |

---

## 3. Persistence & security

| Store | Use |
|-------|-----|
| `storage/user_briefs.pkl` | Per-user brief text (pickle — treat as sensitive). |
| `storage/user_last_brief.pkl` | Last active brief pointer (Slack). |
| `bot_history.db` | Archived conversation sessions. |
| `google_token.pkl` | OAuth for Drive (if used). |

Org62 refresh patterns update **`os.environ`** for Salesforce tokens; they do not rewrite `.env` on disk.

---

## 4. Environment validation

`server.py` runs **`_validate_required_env()`** at import for production entrypoints (Snowflake, LLM key, Salesforce auth). Imports from **`tests/`**, **pytest**, or **`PRODUCT_ADOPTION_SKIP_ENV_VALIDATION`** skip that check so scripts and CI can load the module without a full secret set.

---

## 5. Related docs

- **GM Review product + AI behavior:** `docs/AI_COUNCIL_GM_REVIEW.md`
- **Visual architecture:** `docs/ARCHITECTURE_DIAGRAM.md`
