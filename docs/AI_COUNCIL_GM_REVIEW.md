# AI Council Review (GM Review)

**Product name:** *AI Council Review* describes the executive-style, at-risk renewal review experience.  
**Code name:** *GM Review* — implementation lives under `services/gm_review_workflow.py`, MCP tools `generate_gm_reviews` / `generate_gm_review_canvas`, and Slack `/gm-review-canvas`.

## Purpose

For each account (or opportunity), the workflow:

1. **Resolves** the customer in Salesforce (account name, opp id, or renewal context).
2. **Pulls** structured renewal / red-account / team data from **Org62 (Salesforce)**.
3. **Enriches** with **Snowflake** adoption, ARI-style signals, utilization, and product-line attrition where configured.
4. **Classifies** risk into themes (e.g. platform underutilization, competitive threat) using rules in `domain/intelligence/risk_engine.py`, mapped to **standard playbook recommendations**.
5. **Calls the LLM** (Salesforce LLM Gateway, Claude) twice per account for concise **risk notes** and **tailored recommendations**, with deterministic fallbacks if the LLM is unavailable (circuit breaker or errors).
6. **Renders** a wide **Slack Canvas** markdown table plus optional **Google Sheets** export.

The “council” metaphor: deterministic policy (themes + playbooks) plus an LLM pass that synthesizes the same facts into short, exec-ready language.

## Entry points

| Entry | Location | Notes |
|--------|-----------|--------|
| Slack slash command | `/gm-review-canvas` in `slack_app.py` | Optional cloud token + comma-separated accounts or opp IDs; threaded async `process()`; canvas + optional Sheets |
| MCP tools | `server.py`: `generate_gm_reviews`, `generate_gm_review_canvas` | Same `GMReviewWorkflow`; canvas tool returns combined markdown |
| Tests | `tests/test_commerce_cloud_e2e.py` Section 5 (WF-*) | Live integration; needs SF + Snowflake + LLM env for full pass |

## High-level flow (per account)

```text
Input (name | opp id)
    → Resolve account + renewal opp (Salesforce SOQL via org62_client)
    → Parallel / phased SF + Snowflake work (see gm_review_workflow)
    → format_enrichment_for_display + build_adoption_pov (domain/content)
    → generate_risk_analysis (risk_engine: classify → LLM bullets ×2)
    → Row in combined canvas (build_gm_review_canvas_markdown)
```

Two resolution paths exist inside `_generate_review` (name-based vs opp-id-based); timing and parallelism differ slightly, but outputs are the same shape.

## Main modules

| Layer | Path | Role |
|--------|------|------|
| Orchestration | `services/gm_review_workflow.py` | `GMReviewWorkflow.run()`, caching, `ThreadPoolExecutor`, structured log `account_review` |
| Salesforce | `domain/salesforce/org62_client.py` | Auth, `sf_query`, concurrency semaphore, limit / timeout structured logs |
| Snowflake | `domain/analytics/snowflake_client.py` | `enrich_account`, attrition queries, pool; slow-query structured logs |
| Intelligence | `domain/intelligence/risk_engine.py` | `classify_risk_situation`, `RISK_RECOMMENDATION_MAP`, `generate_risk_analysis` + LLM |
| Presentation | `domain/content/canvas_builder.py` | `build_gm_review_canvas_markdown`, adoption POV helpers |
| Sheets | `domain/integrations/gsheet_exporter.py` | Optional batch export after Slack canvas |
| LLM | `server.py` | `call_llm_gateway` / `call_llm_gateway_with_retry`, TLS via certifi/CA bundle, circuit breaker |

## AI behavior (what the model sees)

`generate_risk_analysis` builds a **structured context block** from Salesforce + Snowflake (risk theme, forecasted attrition, ARI/health/utilization, red account, manager notes, etc.).

- **Risk notes:** prompt asks for three plain-text bullets (length and formatting constraints to keep Canvas/Sheets safe).
- **Recommendations:** prompt supplies **theme + standard playbook lines** and asks for two specific recommendations in the same plain format.

If `call_llm_fn` returns empty or the circuit is open, **fallback** text is derived from the same classification and playbook (no hallucinated metrics).

## Correlation and observability

- Batch runs set a **`run_id`** (UUID) in `GMReviewWorkflow.run()`; each successful account emits **`log_structured("account_review", …)`** with phase timings (`resolve_ms`, `sf_ms`, `snowflake_ms`, `llm_ms`, `total_ms`).
- LLM calls emit **`llm_call`** / circuit events per `log_utils.log_structured`.

## Configuration & environment

**Required for full stack** (also enforced at `server.py` startup for non-test entrypoints):

- Snowflake: `SNOWFLAKE_USER`, `SNOWFLAKE_ACCOUNT` (+ connector secrets as elsewhere in app)
- LLM: `LLM_GATEWAY_API_KEY`; optional `LLM_GATEWAY_CA_BUNDLE` for custom TLS trust
- Salesforce: session + instance **or** username + password (see `.env.example` / `org62_client`)

**Optional:**

- `GSHEET_ID` / `GOOGLE_SHEET_ID` — post-canvas Google Sheets export from Slack command
- `LLM_CIRCUIT_THRESHOLD`, `LLM_CIRCUIT_COOLDOWN` — resilience tuning
- `SF_MAX_CONCURRENT` — global Salesforce REST concurrency

## Operational notes

- **Latency:** Slack command posts a “generating…” message; work runs in a **background thread** (`slack_app.py`).
- **Parallelism:** Up to **8** concurrent accounts in the workflow (`max_concurrent`); SF calls share a process-wide semaphore.
- **Degraded mode:** If the LLM circuit opens, `call_llm_gateway_with_retry` returns `""`; risk_engine uses **fallback** bullets from rules + playbook.

## Related documentation

- System-wide architecture: `docs/ARCHITECTURE.md`
- Diagrams (Mermaid): `docs/ARCHITECTURE_DIAGRAM.md`
- Repo file index: `docs/REPO_LAYOUT.md`
