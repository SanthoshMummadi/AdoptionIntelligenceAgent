# Conversational AI Implementation for Adoption Bot

## Overview
Added natural language message handling to the Slack bot, allowing users to ask adoption questions conversationally instead of using slash commands.

## Implementation Details

### STEP 1: Intent Classifier Function
**File:** `services/adoption_heatmap_workflow.py`

Added `classify_adoption_intent(text, call_llm_fn=None)` function that:
- Uses the existing LLM gateway (from `server.py`) to classify user intent
- Falls back to keyword matching if LLM fails or is unavailable
- Returns structured intent dict with type, cloud, FY, feature group, etc.

**Intent Types Supported:**
- `heatmap_summary` - overall adoption view
- `group_drilldown` - specific feature group
- `feature_detail` - specific feature
- `account_lookup` - specific account/org
- `top_movers` - who is growing/declining
- `feature_owner` - who owns a feature
- `industry_filter` - filter by industry
- `region_filter` - filter by region
- `not_adoption` - not an adoption query

**Key Features:**
- Uses existing `call_llm_gateway_with_retry` from `server.py`
- Respects LLM circuit breaker for degraded mode
- Keyword fallback handles 18+ feature groups
- Returns null for missing fields (not strings)

### STEP 2: Message Event Handler
**File:** `slack_app.py`

**Added Components:**
1. **ADOPTION_KEYWORDS** - Commerce-specific keywords (no generic terms like "how is", "show me")
2. **_is_adoption_query()** - Quick filter before calling LLM
3. **Conversational handler** - Inserted into existing `@app.event("message")` handler

**Behavior:**
- Only responds in DMs or when bot is @mentioned (not all channel messages)
- Shows typing indicator (⏳ reaction)
- Runs in background thread (sync Bolt pattern)
- Routes to appropriate handler based on intent type
- Removes typing indicator when done

**Intent Routing:**
- `heatmap_summary` → `build_adoption_heatmap_blocks()` with full data
- `group_drilldown` → `build_group_drilldown_blocks()` filtered by group
- `feature_detail` → `build_feature_detail_blocks()` with movers data
- `top_movers` → Placeholder (coming soon message)
- `feature_owner` → Placeholder (coming soon message)
- `industry_filter`/`region_filter` → Placeholder (coming soon message)

## Design Decisions

### Why Sync Not Async
The Slack Bolt app uses sync handlers, not async. All handlers are `def` not `async def`, and use threading for background work.

### Why Inside Existing Handler
There's only ONE `@app.event("message")` handler. Added new logic after heatmap thread replies but before session-based brief handling.

### Why Tightened Keywords
Removed generic words ("how is", "show me", "top") that would trigger on non-adoption conversations. Only commerce-specific terms remain.

### Why Thread-Based
Heavy work (Snowflake queries, LLM calls) runs in background thread using `threading.Thread()` pattern already used in `handle_adoption_heatmap()`.

### Why Check DM/Mention
Prevents bot from responding to every message in public channels. Only responds when:
1. Message is in a DM (channel starts with 'D'), OR
2. Bot is explicitly @mentioned in the text

## Test Cases

Test by typing these messages directly in Slack DM (no slash command):

### Test 1: Cart Performance
**Message:** "How is Cart performing?"
**Expected:** Group drill-down for Cart feature group

### Test 2: Overall Adoption
**Message:** "Show me Commerce B2B adoption"
**Expected:** Full heatmap summary for Commerce B2B FY2027

### Test 3: Top Movers
**Message:** "Who are the top movers in Search?"
**Expected:** Placeholder message "Top movers for Search coming soon!"

### Test 4: Buyer Groups
**Message:** "How is Buyer Groups doing?"
**Expected:** Group drill-down for Buyer Groups feature group

### Test 5: Feature Owner
**Message:** "Who owns the Pricing features?"
**Expected:** Placeholder message "Looking up owner for Pricing..."

## Files Modified

1. **services/adoption_heatmap_workflow.py**
   - Added `import json`
   - Added `from log_utils import log_debug`
   - Added `classify_adoption_intent()` function (169 lines)

2. **slack_app.py**
   - Updated imports to include `classify_adoption_intent`
   - Added `ADOPTION_KEYWORDS` constant (14 keywords)
   - Added `_is_adoption_query()` helper function
   - Added conversational handler inside `handle_message()` (249 lines)

## Compatibility

- ✅ Does not break existing slash commands
- ✅ Does not change attrition or GM review handlers
- ✅ Only adds new message handler logic
- ✅ Keyword fallback works without LLM
- ✅ Bot only responds to adoption queries in DMs or when mentioned
- ✅ adoption-claude branch only

## Integration Points

**LLM Gateway:**
- Uses `call_llm_gateway_with_retry()` from `server.py`
- Respects circuit breaker (_llm_circuit_is_open)
- Falls back to keyword matching if LLM unavailable

**Snowflake Data:**
- Calls `get_adoption_heatmap_data()` same as slash command
- Calls `get_feature_account_movers()` for feature details
- Reuses all existing query logic

**Block Kit:**
- Uses `build_adoption_heatmap_blocks()`
- Uses `build_group_drilldown_blocks()`
- Uses `build_feature_detail_blocks()`
- Same visual output as slash commands

## Future Enhancements

1. **Top Movers** - Implement actual movers analysis (growth/decline)
2. **Feature Owner** - Query owner data from Snowflake/SFDC
3. **Industry/Region Filters** - Wire filters through to Snowflake queries
4. **Account Lookup** - Show adoption for specific account/org
5. **Multi-turn Conversation** - Remember context across messages
