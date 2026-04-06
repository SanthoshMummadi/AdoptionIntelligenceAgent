# PM Intelligence Hub – Slack Agent

A Slack-native “PM Intelligence Hub” that helps product teams analyze briefs, run V2MoM-style reviews, and reason about attrition risk – all from a single DM with the bot.

## What’s Implemented

- **Slack Agent + DM Dashboard**
  - Welcome “PM Intelligence Hub” card sent in DM on first open.
  - Full-width module buttons (Product Brief, V2MoM, Attrition Risk, Feature Scorecard) with short descriptions.
  - Natural-language module switching (e.g. “switch to product brief”, “v2mom analysis”, “switch to attrition”).

- **Modules & Prompts (config‑driven)**
  - `config.properties` defines:
    - `[HUB]` modules and their status.
    - `[PRODUCT_BRIEF]` and `[ATTRITION_RISK]` (and future sections) as numbered prompts.
  - Shared helpers:
    - `fetch_hub_items`, `fetch_section_prompts` (from `canvas_prompts.py`).
    - `_send_section_prompts_dm` renders each prompt as a bullet‑style button in DM.

- **Product Brief Analysis**
  - **Entry points**:
    - Click “Product Brief Analysis” on the DM dashboard.
    - Type a module switch phrase like “product brief analysis”.
  - Bot:
    - Sets `user_hub_section[user] = "product_brief"`.
    - Asks user to upload a file or paste a Google Drive link.
  - After upload:
    - Saves the brief to the MCP server (`server.upload_brief_text`).
    - Shows **9 analysis prompts** (from `[PRODUCT_BRIEF]`) as clickable buttons:
      strategy, measurement matrix, controversies, friction, blockers, migration, trial org, support readiness, content generation.

- **V2MoM Analysis**
  - Same UX as Product Brief:
    - `user_hub_section[user] = "v2mom"`.
    - Asks for upload (PDF / Excel / CSV / Google Drive link).
    - After upload, shows V2MoM‑specific prompts (from a separate config section) via `_send_section_prompts_dm`.

- **Attrition Risk Predictor**
  - Mode is toggled via:
    - “Attrition Risk Predictor” card.
    - Switch phrases like “attrition risk” or “churn risk”.
  - **Two behaviors**:
    - If the message looks like an **account name** → runs Excel‑driven lookup from `MASTER_FILE` and prints a rich risk summary for each product line.
    - If the message looks like a **question** → routes to brief Q&A with contextual prompt (“user is analyzing attrition risk”).
  - Also supports org62-style Salesforce queries + GMV enrichment:
    - At-risk renewals output in both “clean” and “enhanced” formats
    - Risk situation → recommendation theme mapping (shown in results + canvases)
    - Slack canvas generation via `/at-risk-canvas`

- **File Ingestion (PDF / Excel / CSV / Drive)**
  - `file_shared` event handler:
    - Accepts:
      - 📄 PDF (via `PyPDF2`).
      - 📊 Excel `.xlsx` / `.xls` (via `pandas.read_excel`).
      - 📋 CSV (decoded to text).
    - Normalizes text and uploads to MCP via `server.upload_brief_text`.
    - Infers active module from `user_hub_section` and:
      - Confirms upload (file name, rows/pages, character count).
      - Immediately shows prompts for the selected module.
  - Google Drive links:
    - Handled by `read_any_drive_link` using Google OAuth token.
    - Supports Docs / Sheets / Slides export -> PDF -> text.

- **Session & History**
  - In‑memory `active_sessions` plus SQLite `conversation_history` for long‑term storage.
  - `clear_session`:
    - Flushes conversation to DB.
    - Resets “welcome shown” flag so the DM dashboard reappears next time.
  - Lightweight `user_sessions` store in `storage/user_sessions.pkl`:
    - Tracks `welcomed` flag and timestamps.

## Key Files

- `slack_app.py` – main Slack Bolt app:
  - Slash commands, message events, file uploads, module routing, attrition logic.
  - DM dashboard helpers and hub menu rendering.
- `canvas_prompts.py` – reads prompts/hub config from `config.properties`.
- `home.py` – legacy App Home view (kept for reference; current experience is DM‑based).
- `config.properties` – hub modules and per‑module prompt definitions.
- `.env` – runtime configuration (see **`.env.example`** for keys; never commit `.env`).
  - `SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`
  - `LLM_GATEWAY_API_KEY`, `LLM_MODEL=claude-3-7-sonnet`
  - TLS: **Internal** Salesforce LLM gateway URLs use **`LLM_GATEWAY_CA_BUNDLE`** if set, otherwise **verify is disabled** (corporate CA is not in certifi). Other URLs use **certifi** / custom CA; `LLM_GATEWAY_VERIFY=false` only affects messaging for non-internal URLs (verify stays on).
  - `TABLEAU_*` credentials (for Tableau integrations).
- `project_paths.py` – `PROJECT_ROOT` for portable `load_dotenv(PROJECT_ROOT / ".env")` (no hardcoded home paths).
- **Architecture docs**
  - `docs/ARCHITECTURE.md` — components and data paths
  - `docs/ARCHITECTURE_DIAGRAM.md` — Mermaid + ASCII diagrams (Slack, MCP, GM Review)
  - `docs/AI_COUNCIL_GM_REVIEW.md` — **AI Council Review** (= GM Review): flow, AI behavior, entry points, ops
  - `docs/REPO_LAYOUT.md` — file index and entrypoints

### Security & portability notes

- **Pickle** (`user_briefs.pkl`, `google_token.pkl`) — treat files as sensitive; don’t share or run from untrusted paths.
- **Org62**: `org62_client` refreshes the session via `sf org display` and updates **`os.environ` only** (it does **not** rewrite `.env`).
- **Experimental scripts** (`explore*.py`, some `fix_*.py`) are not maintained as product code; see `docs/REPO_LAYOUT.md`.

## Running the Bot (Local)

1. **Install dependencies**

```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

2. **Configure environment**

- Create `.env` with:
  - `SLACK_BOT_TOKEN`
  - `SLACK_APP_TOKEN`
  - `LLM_GATEWAY_API_KEY`
  - `LLM_MODEL=claude-3-7-sonnet`
  - `TABLEAU_SERVER`, `TABLEAU_SITE`, `TABLEAU_TOKEN_NAME`, `TABLEAU_TOKEN_SECRET`

3. **Google OAuth (for Drive links)**

```bash
python google_auth.py
```

This will open a browser flow and store `google_token.pkl` next to the app.

4. **Start the Slack app**

```bash
python slack_app.py
```

The bot runs via Socket Mode and listens for DMs, file uploads, and App Home openings.

## How to Use (End‑User View)

- DM the bot and:
  - Click **“Product Brief Analysis”** or **“V2MoM Analysis”**, then upload a file (or paste a Drive link) and choose a prompt.
  - Click **“Attrition Risk Predictor”** and type an account name to see risk summaries from the Excel model.
  - Type natural phrases like **“switch to product brief”** or **“switch to v2mom”** to jump between modules.

The bot will automatically surface the right prompt set and route your questions either to the Excel‑driven attrition engine or the brief Q&A engine behind the MCP server.

~~~ markdown
Adoption Intelligence Bot



AI-powered Slack bot for analyzing product briefs using Salesforce LLM Gateway.



Features



- :page_facing_up: Upload product brief PDFs directly in Slack
- :robot_face: AI-powered analysis using Claude 4.5 Sonnet
- :bar_chart: Generate intelligent summaries
- :question: Ask natural language questions about briefs
- :mag: Extract specific sections with AI
- :bust_in_silhouette: User isolation - each user has private brief collections
- :thread: Thread-aware commands



Setup



1. Install Dependencies


cd product-adoption-mcp
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
~~~

*2. Set Environment Variables*

export LLM_GATEWAY_API_KEY='YOUR_LLM_GATEWAY_API_KEY'
export SLACK_BOT_TOKEN='xoxb-your-token'
export SLACK_APP_TOKEN='xapp-your-token'
*3. Run the Bot*

python slack_app.py
Extract text from all pages
Store it in your private collection
Respond with usage instructions
List all your uploaded briefs
Get AI-generated comprehensive summary
Ask questions about the brief
Example: /query my-brief: What are the adoption risks?
Extract sections related to a keyword
Example: /extract my-brief keyword: adoption
summarize - Auto-detects the brief
query: What are the risks? - Auto-detects the brief
extract keyword: adoption - Auto-detects the brief

*Usage*

*Upload a PDF*
Simply drag and drop a PDF file into Slack. The bot will:

*Slash Commands*

*`/attrition-risk <Account Name>`*

*`/attrition-clouds`*

*`/at-risk-canvas <optional filters>`*

*`/risk-mapping <optional theme>`*

*`/tableau-test`*

*Thread-Aware Commands*

After uploading a PDF, you can reply in the thread with simplified commands:

*Architecture*

User uploads PDF → Slack App → MCP Server → LLM Gateway → Response
Slack App: Handles user interactions
MCP Server: Manages brief storage and processing
LLM Gateway: Provides AI analysis (Claude 4.5 Sonnet)
50 requests per minute per user (Salesforce LLM Gateway)


*Rate Limits*


*Support*

Questions? Post in #adoption-agent-beta
~~~

-------------------------

**5. How to Run Everything**

1. Navigate to project
cd ~/Desktop/product-adoption-mcp



2. Activate virtual environment
source venv/bin/activate



3. Install/update dependencies
pip install -r requirements.txt



4. Set environment variables
export LLM_GATEWAY_API_KEY='YOUR_LLM_GATEWAY_API_KEY'
export SLACK_BOT_TOKEN='xoxb-your-token-here'
export SLACK_APP_TOKEN='xapp-your-token-here'



5. Run the Slack bot
python slack_app.py



OR to test MCP server in Inspector:
npx @modelcontextprotocol/inspector python server.py
~~~



-------------------------



This is the complete, production-ready code with:
:white_check_mark: LLM Gateway integration (Claude 4.5 Sonnet)
:white_check_mark: User isolation
:white_check_mark: Thread-aware commands
:white_check_mark: Automatic PDF processing
:white_check_mark: Slash commands
:white_check_mark: Error handling
:white_check_mark: Full documentation



Ready to deploy! :rocket: