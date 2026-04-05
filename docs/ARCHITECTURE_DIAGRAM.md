# Architecture diagrams

Companion to **`docs/ARCHITECTURE.md`**. Diagrams use [Mermaid](https://mermaid.js.org/) (renders in GitHub, many IDEs, and Cursor).

---

## 1. System context (C4-style)

Actors and external systems touching this repository.

```mermaid
flowchart LR
  subgraph Users
    PM[Product / PM users]
  end

  subgraph Salesforce["Salesforce (Org62)"]
    SFAPI[REST / SOQL]
  end

  subgraph Data["Analytics"]
    SFDB[(Snowflake)]
  end

  subgraph AI["Corporate AI"]
    GW[LLM Gateway<br/>Claude chat/completions]
  end

  subgraph SlackInc["Slack"]
    SC[Channels / DM / Canvas]
  end

  subgraph Optional["Optional"]
    GS[(Google Sheets)]
    GDrv[Google Drive]
  end

  PM --> SC
  SC <--> Bot[Adoption Intelligence<br/>slack_app.py + server.py]
  Bot --> SFAPI
  Bot --> SFDB
  Bot --> GW
  Bot --> GS
  Bot --> GDrv
```

---

## 2. Application containers

Two primary processes; MCP may run standalone (Inspector/CLI) while Slack runs Socket Mode.

```mermaid
flowchart TB
  subgraph ProcessSlack["Process: slack_app.py"]
    Bolt[Slack Bolt<br/>Socket Mode]
    Bolt --> Sess[(user_sessions.pkl<br/>user_last_brief.pkl)]
    Bolt --> Hist[(bot_history.db)]
    Bolt --> SrvImport[import server]
  end

  subgraph ProcessMCP["Process: server.py (FastMCP)"]
    MCP[FastMCP tools]
    Briefs[(storage/user_briefs.pkl)]
    LLMClient[LLM session + circuit breaker]
    MCP --> Briefs
    MCP --> LLMClient
  end

  SrvImport -.->|same interpreter<br/>when Bolt loads server| ProcessMCP

  LLMClient --> GW[LLM Gateway HTTPS]
  MCP --> SF[org62 / Salesforce]
  MCP --> SN[Snowflake client]
  Bolt --> Canvas[Slack Canvas API]
```

---

## 3. GM Review / AI Council data flow

End-to-end for one batch of accounts (simplified).

```mermaid
sequenceDiagram
  participant U as User / MCP client
  participant S as slack_app or server MCP
  participant W as GMReviewWorkflow
  participant SF as org62_client
  participant SN as snowflake_client
  participant R as risk_engine
  participant L as LLM Gateway

  U->>S: Accounts or Opp IDs
  S->>W: run(inputs, cloud, …)
  loop Per account (parallel pool)
    W->>SF: Resolve + renewal + red + team
    W->>SN: enrich_account + attrition
    W->>R: generate_risk_analysis(context)
    R->>L: Risk notes prompt
    L-->>R: Bullets
    R->>L: Recommendations prompt
    L-->>R: Bullets
    R-->>W: risk_notes, recommendation
  end
  W->>W: build_gm_review_canvas_markdown
  W-->>S: reviews + combined_canvas
  S->>U: Canvas / text / optional Sheets
```

---

## 4. LLM path (brief Q&A vs GM Review)

Shared client; GM Review uses the same gateway with shorter max_tokens in `risk_engine`.

```mermaid
flowchart LR
  subgraph Callers
    QB[query_brief MCP tool]
    GM[generate_risk_analysis]
  end

  QB --> RETRY[call_llm_gateway_with_retry]
  GM --> RETRY
  RETRY --> CB{Circuit open?}
  CB -->|yes| Empty[Return empty string]
  CB -->|no| POST[session.post verify=certifi or CA bundle]
  POST --> GW[LLM Gateway]
```

---

## 5. ASCII summary (quick paste)

```text
                    ┌─────────────────┐
                    │  Slack / MCP    │
                    │  clients        │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
       ┌──────▼──────┐               ┌──────▼──────┐
       │ slack_app   │──imports──▶  │  server.py  │
       │ Bolt + UI   │               │ FastMCP     │
       └──────┬──────┘               │ + LLM       │
              │                      └──────┬──────┘
              │                             │
              └──────────────┬──────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         ▼                   ▼                   ▼
   ┌──────────┐       ┌────────────┐      ┌──────────┐
   │ Salesforce│       │ Snowflake  │      │ LLM GW   │
   │ org62     │       │ analytics  │      │ HTTPS    │
   └──────────┘       └────────────┘      └──────────┘

   GM Review = GMReviewWorkflow + risk_engine + canvas_builder
```

---

For narrative and file references, see **`docs/ARCHITECTURE.md`** and **`docs/AI_COUNCIL_GM_REVIEW.md`**.
