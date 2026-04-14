"""
tests/test_commerce_cloud_e2e.py
End-to-end checks for Commerce Cloud attrition / GM review stack.

Run from repo root:
  python3 tests/test_commerce_cloud_e2e.py

Optional:
  RUN_GSHEET_E2E=1  — live Google Sheet open + export (mutates sheet)
"""
from __future__ import annotations

import os
import sys
import time
import traceback
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()


class SkipTest(Exception):
    """Skip a live-data-dependent test without failing the run."""


results: dict = {
    "passed": [],
    "failed": [],
    "skipped": [],
    "start_time": datetime.now(),
}


def test(name: str):
    def decorator(fn):
        def wrapper():
            print(f"\n  🧪 {name}")
            try:
                fn()
                results["passed"].append(name)
                print("  ✅ PASSED")
            except SkipTest as e:
                results["skipped"].append((name, str(e)))
                print(f"  ⏭️  SKIPPED: {e}")
            except AssertionError as e:
                results["failed"].append((name, str(e)))
                print(f"  ❌ FAILED: {e}")
            except Exception as e:
                results["failed"].append((name, str(e)))
                print(f"  ❌ ERROR: {str(e)[:100]}")
                traceback.print_exc()

        return wrapper

    return decorator


def _row_val(row: dict, *keys: str):
    for k in keys:
        if k in row:
            return row[k]
        kl = k.lower()
        for rk, rv in row.items():
            if str(rk).lower() == kl:
                return rv
    return None


# ── Test accounts (org-specific; adjust if your org differs) ─────────────
TEST_ACCOUNTS = {
    "adidas": {
        "name": "Adidas AG",
        "id": "00130000002xFEIAA2",
        "expected_territory": "EMEA",
    },
    "oxford": {
        "name": "Oxford Industries Inc.",
        "id": "00100000001iI94AAE",
        "expected_territory": "AMER",
    },
}

OPP_ID = "0063y00001ANfq2AAD"  # Adidas renewal opp (verify in your org)

print("=" * 70)
print("COMMERCE CLOUD ATTRITION BOT — END-TO-END TESTS")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)

# ══════════════════════════════════════════════════════════════════════
# SECTION 1: SALESFORCE
# ══════════════════════════════════════════════════════════════════════
print("\n📦 SECTION 1: Salesforce Connection & Account Resolution")


@test("SF-001: Salesforce connection via session token")
def test_sf_connection():
    from domain.salesforce.org62_client import sf_query

    result = sf_query("SELECT Id FROM User LIMIT 1")
    assert result.get("records"), "No records returned"


@test("SF-002: Resolve account by exact name (Adidas AG)")
def test_resolve_exact():
    from domain.salesforce.org62_client import resolve_account

    acct = resolve_account("Adidas AG")
    assert acct, "Account not found"
    assert acct["id"] == TEST_ACCOUNTS["adidas"]["id"], (
        f"Wrong account ID: {acct['id']}"
    )
    assert acct["name"] == "Adidas AG"


@test("SF-003: Resolve account by partial name (Oxford)")
def test_resolve_partial():
    from domain.salesforce.org62_client import resolve_account

    acct = resolve_account("Oxford Industries")
    assert acct, "Account not found"
    assert "Oxford" in acct["name"]


@test("SF-004: Resolve account enhanced with Snowflake fallback")
def test_resolve_enhanced():
    from domain.salesforce.org62_client import resolve_account_enhanced

    acct = resolve_account_enhanced("Adidas AG", cloud="Commerce Cloud")
    assert acct, "Account not found"
    assert acct["id"] == TEST_ACCOUNTS["adidas"]["id"]


@test("SF-005: Resolve non-existent account returns None")
def test_resolve_nonexistent():
    from domain.salesforce.org62_client import resolve_account

    acct = resolve_account("ZZZZNONEXISTENTACCOUNT99999")
    assert acct is None, f"Expected None, got: {acct}"


@test("SF-006: Get renewal opportunities — Commerce Cloud filter")
def test_renewal_opps_cloud():
    from domain.salesforce.org62_client import get_renewal_opportunities

    opps = get_renewal_opportunities(
        TEST_ACCOUNTS["adidas"]["id"], "Commerce Cloud"
    )
    assert len(opps) > 0, "No renewal opps found"
    assert "Commerce" in opps[0]["Name"]


@test("SF-007: Get renewal opportunities — any cloud fallback")
def test_renewal_opps_any():
    from domain.salesforce.org62_client import get_renewal_opportunities_any_cloud

    opps = get_renewal_opportunities_any_cloud(TEST_ACCOUNTS["adidas"]["id"])
    assert len(opps) > 0, "No renewal opps found"


@test("SF-008: Get red account record (if present)")
def test_red_account():
    from domain.salesforce.org62_client import get_red_account

    red = get_red_account(TEST_ACCOUNTS["adidas"]["id"])
    if red is None:
        raise SkipTest("No Red_Account__c row for Adidas in this org")
    stage = red.get("Stage__c")
    assert stage in ("Open", "Precautionary", None) or red.get("_historical"), (
        f"Unexpected stage: {stage}"
    )


@test("SF-009: Get account team")
def test_account_team():
    from domain.salesforce.org62_client import get_account_team

    team = get_account_team(TEST_ACCOUNTS["adidas"]["id"])
    assert isinstance(team, dict), "Expected dict"
    assert "ae" in team


for fn in [
    test_sf_connection,
    test_resolve_exact,
    test_resolve_partial,
    test_resolve_enhanced,
    test_resolve_nonexistent,
    test_renewal_opps_cloud,
    test_renewal_opps_any,
    test_red_account,
    test_account_team,
]:
    fn()

# ══════════════════════════════════════════════════════════════════════
# SECTION 2: SNOWFLAKE
# ══════════════════════════════════════════════════════════════════════
print("\n📦 SECTION 2: Snowflake Data Layer")


@test("SF2-001: Snowflake singleton connection")
def test_snow_connection():
    from domain.analytics.snowflake_client import run_query

    rows = run_query("SELECT 1 AS ok")
    assert rows, "No rows"
    v = _row_val(rows[0], "OK", "ok")
    assert v == 1, f"Expected 1, got {v!r}"


@test("SF2-002: ARI score by opportunity ID (15-char)")
def test_ari_by_opp():
    from domain.analytics.snowflake_client import get_ari_score, to_15_char_id

    opp_id_15 = to_15_char_id(OPP_ID)
    ari = get_ari_score(opp_id_15)
    assert ari is not None, "No ARI data found"
    cat = _row_val(ari, "ATTRITION_PROBA_CATEGORY", "attrition_proba_category")
    assert cat in ("High", "Medium", "Low"), f"Invalid category: {cat!r}"
    assert _row_val(ari, "ATTRITION_PROBA", "attrition_proba") is not None


@test("SF2-003: ARI score by account ID")
def test_ari_by_account():
    from domain.analytics.snowflake_client import get_ari_score_by_account, to_15_char_id

    acct_id_15 = to_15_char_id(TEST_ACCOUNTS["adidas"]["id"])
    rows = get_ari_score_by_account(acct_id_15, "Commerce Cloud")
    assert len(rows) > 0, "No ARI rows found"


@test("SF2-004: Customer health score")
def test_health_score():
    from domain.analytics.snowflake_client import get_customer_health, to_15_char_id

    acct_id_15 = to_15_char_id(TEST_ACCOUNTS["adidas"]["id"])
    health = get_customer_health(acct_id_15)
    assert health is not None, "No health data"
    assert health.get("overall_score") is not None
    assert health.get("overall_literal") is not None


@test("SF2-005: Renewal AOV from WV_CI_RENEWAL_OPTY_VW")
def test_renewal_aov():
    from domain.analytics.snowflake_client import get_renewal_aov, to_15_char_id

    opp_id_15 = to_15_char_id(OPP_ID)
    renewal = get_renewal_aov(opp_id_15)
    assert renewal, "No renewal data"
    assert float(renewal.get("renewal_aov", 0) or 0) > 0, "AOV is 0"
    assert float(renewal.get("renewal_atr_snow", 0) or 0) >= 0, "ATR_SNOW missing"
    assert renewal.get("csg_geo") is not None, "csg_geo key missing"


@test("SF2-006: Usage / utilization from CIDM")
def test_usage_gmv():
    from domain.analytics.snowflake_client import get_usage_unified, to_15_char_id

    acct_id_15 = to_15_char_id(TEST_ACCOUNTS["adidas"]["id"])
    usage = get_usage_unified(acct_id_15, "Commerce Cloud").get("summary") or {}
    assert usage, "No usage data"
    src = usage.get("source")
    assert src in ("GMV", "Commerce aggregate", "All products"), (
        f"Unexpected source: {src!r}"
    )
    util_s = str(usage.get("utilization_rate", "0%")).rstrip("%").strip()
    util = float(util_s)
    assert util > 0, "Utilization is 0"


@test("SF2-007: Product attrition with cloud filter")
def test_product_attrition_cloud():
    from domain.analytics.snowflake_client import get_account_attrition, to_15_char_id

    acct_id_15 = to_15_char_id(TEST_ACCOUNTS["adidas"]["id"])
    products = get_account_attrition(acct_id_15, "Commerce Cloud")
    assert len(products) > 0, "No products found"


@test("SF2-008: Product attrition without cloud filter")
def test_product_attrition_all():
    from domain.analytics.snowflake_client import get_account_attrition, to_15_char_id

    acct_id_15 = to_15_char_id(TEST_ACCOUNTS["adidas"]["id"])
    products = get_account_attrition(acct_id_15, cloud=None)
    assert len(products) >= 1, f"Expected at least 1 product, got {len(products)}"


@test("SF2-009: Success Plans excluded from overall ARI helper")
def test_success_plans_excluded():
    from domain.analytics.snowflake_client import (
        calculate_overall_ari,
        get_account_attrition,
        to_15_char_id,
    )

    acct_id_15 = to_15_char_id(TEST_ACCOUNTS["adidas"]["id"])
    products = get_account_attrition(acct_id_15, "Commerce Cloud")
    ari = calculate_overall_ari(products)
    top_product = str(ari.get("top_product") or "")
    assert "success plan" not in top_product.lower(), (
        f"Success Plan surfaced as top product: {top_product}"
    )


@test("SF2-010: Full enrichment for account")
def test_full_enrichment():
    from domain.analytics.snowflake_client import (
        enrich_account,
        format_enrichment_for_display,
    )

    enrichment = enrich_account(
        TEST_ACCOUNTS["adidas"]["id"],
        OPP_ID,
        "Commerce Cloud",
    )
    display = format_enrichment_for_display(enrichment)
    assert display.get("ari_category") != "Unknown", "ARI Unknown"
    assert display.get("health_score") is not None, "Health None"
    assert display.get("cc_aov") != "Unknown", "AOV Unknown"
    assert display.get("utilization_rate") != "N/A", "Util N/A"


@test("SF2-011: Raw usage data for adoption POV")
def test_usage_raw():
    from domain.analytics.snowflake_client import get_usage_unified, to_15_char_id

    acct_id_15 = to_15_char_id(TEST_ACCOUNTS["adidas"]["id"])
    rows = get_usage_unified(acct_id_15, "Commerce Cloud").get("raw_rows") or []
    assert len(rows) > 0, "No usage rows"


@test("SF2-012: fmt_amount formatting")
def test_fmt_amount():
    from domain.analytics.snowflake_client import fmt_amount

    assert fmt_amount(1608311) == "$1.6M"
    assert fmt_amount(695492) == "$0.7M"
    assert fmt_amount(0) == "$0"
    assert fmt_amount(5000000) == "$5.0M"


for fn in [
    test_snow_connection,
    test_ari_by_opp,
    test_ari_by_account,
    test_health_score,
    test_renewal_aov,
    test_usage_gmv,
    test_product_attrition_cloud,
    test_product_attrition_all,
    test_success_plans_excluded,
    test_full_enrichment,
    test_usage_raw,
    test_fmt_amount,
]:
    fn()

# ══════════════════════════════════════════════════════════════════════
# SECTION 3: FILTER PARSER
# ══════════════════════════════════════════════════════════════════════
print("\n📦 SECTION 3: Filter Parser")


@test("FP-001: Default cloud is Commerce Cloud")
def test_default_cloud():
    from filter_parser import parse_filters

    f = parse_filters("Adidas AG")
    assert f["cloud"] == "Commerce Cloud"
    assert f.get("cloud_explicit") is False


@test("FP-002: Detect cloud from text")
def test_detect_cloud():
    from filter_parser import parse_filters

    f = parse_filters("B2C Commerce, Adidas AG")
    assert f["cloud"] == "B2C Commerce"
    assert f.get("cloud_explicit") is True


@test("FP-003: Detect ARI filter")
def test_ari_filter():
    from filter_parser import parse_filters

    f = parse_filters("ari:high Commerce Cloud")
    assert f["ari_filter"] == "High"


@test("FP-004: Detect ATR threshold")
def test_atr_threshold():
    from filter_parser import parse_filters

    f = parse_filters(">500k Commerce Cloud")
    assert f["min_attrition"] == 500000


@test("FP-005: Detect top N limit")
def test_top_n():
    from filter_parser import parse_filters

    f = parse_filters("top 20 Commerce Cloud")
    assert f["limit"] == 20


@test("FP-006: Extract opp IDs from text")
def test_opp_id_extract():
    from filter_parser import parse_filters

    f = parse_filters(f"Commerce Cloud, {OPP_ID}")
    assert OPP_ID in f["opp_ids"]


@test("FP-007: Manual account parts extraction")
def test_manual_parts():
    from filter_parser import parse_filters

    f = parse_filters("Commerce Cloud, Adidas AG, Oxford Industries")
    assert f["is_manual"] is True
    assert len(f["manual_account_parts"]) == 2


for fn in [
    test_default_cloud,
    test_detect_cloud,
    test_ari_filter,
    test_atr_threshold,
    test_top_n,
    test_opp_id_extract,
    test_manual_parts,
]:
    fn()

# ══════════════════════════════════════════════════════════════════════
# SECTION 4: CANVAS BUILDER
# ══════════════════════════════════════════════════════════════════════
print("\n📦 SECTION 4: Canvas Builder")


@test("CB-001: build_account_brief_blocks returns blocks list")
def test_brief_blocks():
    from domain.content.canvas_builder import build_account_brief_blocks

    blocks = build_account_brief_blocks(
        account={"name": "Test Account", "id": "001TEST", "product_attrition": []},
        opp={
            "Name": "Test Renewal",
            "CloseDate": "2027-01-31",
            "StageName": "Negotiation",
            "Forecasted_Attrition__c": 500000,
        },
        red_account=None,
        snowflake_display={
            "ari_emoji": ":red_circle:",
            "ari_category": "High",
            "ari_probability": "77.3%",
            "health_display": "Yellow (63)",
            "health_score": 63,
            "cc_aov": "$1.6M",
            "utilization_rate": "87.2%",
            "util_emoji": ":large_green_circle:",
        },
        risk_notes="- Risk note 1\n- Risk note 2",
        recommendation="- Action 1\n- Action 2",
        tldr="Executive summary here",
    )
    assert len(blocks) > 0, "No blocks returned"
    assert blocks[0]["type"] == "header", "First block should be header"
    header_text = blocks[0]["text"]["text"]
    assert "Test Account" in header_text


@test("CB-002: Executive summary (TL;DR) not first block")
def test_tldr_at_bottom():
    from domain.content.canvas_builder import build_account_brief_blocks

    blocks = build_account_brief_blocks(
        account={"name": "Test", "id": "001TEST", "product_attrition": []},
        opp={},
        red_account=None,
        snowflake_display={
            "ari_emoji": ":white_circle:",
            "ari_category": "Unknown",
            "ari_probability": "N/A",
            "health_display": "Unknown",
            "cc_aov": "Unknown",
            "utilization_rate": "N/A",
            "util_emoji": ":white_circle:",
        },
        risk_notes="",
        recommendation="",
        tldr="My TL;DR summary",
    )
    tldr_idx = next(
        i
        for i, b in enumerate(blocks)
        if "TL;DR" in str(b) or "Executive" in str(b)
    )
    assert tldr_idx > 0, "Summary block should not be first"


@test("CB-003: build_adoption_pov from usage data")
def test_adoption_pov():
    from domain.content.canvas_builder import build_adoption_pov

    usage_data = [
        {
            "DRVD_APM_LVL_1": "Commerce",
            "DRVD_APM_LVL_2": "B2C Commerce",
            "GRP": "GMV",
            "TYPE": "Commerce Cloud - Digital - GMV-EUR",
            "PROVISIONED": 300000000,
            "ACTIVATED": None,
            "USED": 261713220,
        },
    ]
    pov = build_adoption_pov(usage_data, "Commerce Cloud")
    assert "B2C Commerce" in pov
    assert "provisioned" in pov
    assert "utilized" in pov


@test("CB-004: Success Plans excluded from adoption POV")
def test_adoption_pov_no_success_plans():
    from domain.content.canvas_builder import build_adoption_pov

    usage_data = [
        {
            "DRVD_APM_LVL_1": "Commerce",
            "DRVD_APM_LVL_2": "Commerce - Success Plans - Premier",
            "PROVISIONED": 100000,
            "ACTIVATED": None,
            "USED": 50000,
        },
        {
            "DRVD_APM_LVL_1": "Commerce",
            "DRVD_APM_LVL_2": "B2C Commerce",
            "PROVISIONED": 300000000,
            "ACTIVATED": None,
            "USED": 261713220,
        },
    ]
    pov = build_adoption_pov(usage_data, "Commerce Cloud")
    assert "Success Plans" not in pov, "Success Plans should be excluded!"
    assert "B2C Commerce" in pov


@test("CB-005: build_gm_review_canvas_markdown returns table")
def test_gm_review_markdown():
    from domain.content.canvas_builder import build_gm_review_canvas_markdown

    reviews = [
        {
            "account_id": "001TEST",
            "account_name": "Test Account",
            "opp": {
                "Name": "Test Renewal",
                "CloseDate": "2027-01-31",
                "StageName": "01 Initiate",
                "Forecasted_Attrition__c": 500000,
                "Swing__c": 100000,
                "License_At_Risk_Reason__c": "Financial",
            },
            "snowflake_display": {
                "ari_category": "High",
                "ari_probability": "77.3%",
                "gmv_rate": "12.0%",
                "utilization_rate": "87.2%",
            },
            "enrichment": {
                "renewal_aov": {
                    "renewal_aov": 1608311,
                    "renewal_atr_snow": 810000,
                    "csg_geo": "EMEA",
                }
            },
            "product_attrition": [],
            "all_products_attrition": [
                {"APM_LVL_1": "Commerce"},
                {"APM_LVL_1": "Sales"},
            ],
            "red_account": {"Stage__c": "Precautionary", "Days_Red__c": 17},
            "risk_notes": "- Risk note",
            "recommendation": "- Recommendation",
        }
    ]
    md = build_gm_review_canvas_markdown(reviews=reviews, cloud="Commerce Cloud")
    assert "# Commerce Cloud — GM Review" in md
    assert "At-Risk Renewals" in md
    assert "ACCOUNT" in md
    assert "Test Account" in md
    assert "Account Links" in md
    assert "Summary" in md


for fn in [
    test_brief_blocks,
    test_tldr_at_bottom,
    test_adoption_pov,
    test_adoption_pov_no_success_plans,
    test_gm_review_markdown,
]:
    fn()

# ══════════════════════════════════════════════════════════════════════
# SECTION 5: GM REVIEW WORKFLOW
# ══════════════════════════════════════════════════════════════════════
print("\n📦 SECTION 5: GM Review Workflow (End-to-End)")


@test("WF-001: Workflow resolves account name correctly")
def test_workflow_resolve():
    from server import call_llm_gateway
    from services.gm_review_workflow import GMReviewWorkflow

    wf = GMReviewWorkflow(call_llm_fn=call_llm_gateway, max_concurrent=2)
    out = wf.run(["Adidas AG"], cloud="Commerce Cloud")
    reviews = out.get("reviews", [])
    assert len(reviews) == 1, f"Expected 1 review, got {len(reviews)}"
    assert reviews[0]["account_name"] == "Adidas AG"
    assert reviews[0]["account_id"] == TEST_ACCOUNTS["adidas"]["id"]


@test("WF-002: Workflow resolves by Opp ID")
def test_workflow_opp_id():
    from server import call_llm_gateway
    from services.gm_review_workflow import GMReviewWorkflow

    wf = GMReviewWorkflow(call_llm_fn=call_llm_gateway, max_concurrent=2)
    out = wf.run([OPP_ID], cloud="Commerce Cloud")
    reviews = out.get("reviews", [])
    assert len(reviews) == 1
    assert "Adidas" in reviews[0]["account_name"]


@test("WF-003: Workflow enrichment data is complete")
def test_workflow_enrichment():
    from server import call_llm_gateway
    from services.gm_review_workflow import GMReviewWorkflow

    wf = GMReviewWorkflow(call_llm_fn=call_llm_gateway, max_concurrent=2)
    out = wf.run(["Adidas AG"], cloud="Commerce Cloud")
    review = out["reviews"][0]
    display = review.get("snowflake_display", {})
    assert display.get("ari_category") != "Unknown", "ARI Unknown"
    assert display.get("health_score") is not None, "Health None"
    assert display.get("cc_aov") != "Unknown", "AOV Unknown"
    assert display.get("utilization_rate") != "N/A", "Util N/A"


@test("WF-004: Workflow generates risk notes and recommendations")
def test_workflow_ai():
    from server import call_llm_gateway
    from services.gm_review_workflow import GMReviewWorkflow

    wf = GMReviewWorkflow(call_llm_fn=call_llm_gateway, max_concurrent=2)
    out = wf.run(["Adidas AG"], cloud="Commerce Cloud")
    review = out["reviews"][0]
    assert review.get("risk_notes"), "No risk notes"
    assert review.get("recommendation"), "No recommendation"
    assert len(review["risk_notes"]) > 20, "Risk notes too short"


@test("WF-005: Workflow generates adoption POV")
def test_workflow_adoption_pov():
    from server import call_llm_gateway
    from services.gm_review_workflow import GMReviewWorkflow

    wf = GMReviewWorkflow(call_llm_fn=call_llm_gateway, max_concurrent=2)
    out = wf.run(["Adidas AG"], cloud="Commerce Cloud")
    review = out["reviews"][0]
    pov = review.get("adoption_pov", "")
    assert "B2C Commerce" in pov, f"No B2C Commerce in POV: {pov[:200]!r}"
    assert "provisioned" in pov


@test("WF-006: Workflow handles multiple accounts in parallel")
def test_workflow_parallel():
    from server import call_llm_gateway
    from services.gm_review_workflow import GMReviewWorkflow

    wf = GMReviewWorkflow(call_llm_fn=call_llm_gateway, max_concurrent=3)
    start = time.time()
    out = wf.run(["Adidas AG", "Oxford Industries"], cloud="Commerce Cloud")
    elapsed = time.time() - start
    reviews = out.get("reviews", [])
    assert len(reviews) == 2, f"Expected 2 reviews, got {len(reviews)}"
    assert elapsed < 120, f"Too slow: {elapsed:.1f}s"
    print(f"    ⏱️  {elapsed:.1f}s for 2 accounts")


@test("WF-007: Workflow handles invalid account gracefully")
def test_workflow_invalid():
    from server import call_llm_gateway
    from services.gm_review_workflow import GMReviewWorkflow

    wf = GMReviewWorkflow(call_llm_fn=call_llm_gateway, max_concurrent=2)
    out = wf.run(["ZZZZNONEXISTENTACCOUNT99999"], cloud="Commerce Cloud")
    reviews = out.get("reviews", [])
    assert len(reviews) == 0, "Should return 0 reviews for invalid account"


@test("WF-008: Workflow generates combined canvas markdown")
def test_workflow_canvas():
    from server import call_llm_gateway
    from services.gm_review_workflow import GMReviewWorkflow

    wf = GMReviewWorkflow(call_llm_fn=call_llm_gateway, max_concurrent=2)
    out = wf.run(["Adidas AG"], cloud="Commerce Cloud")
    canvas = out.get("combined_canvas", "")
    assert "GM Review" in canvas
    assert "At-Risk Renewals" in canvas
    assert "Adidas AG" in canvas


for fn in [
    test_workflow_resolve,
    test_workflow_opp_id,
    test_workflow_enrichment,
    test_workflow_ai,
    test_workflow_adoption_pov,
    test_workflow_parallel,
    test_workflow_invalid,
    test_workflow_canvas,
]:
    fn()

# ══════════════════════════════════════════════════════════════════════
# SECTION 6: GOOGLE SHEETS (optional live)
# ══════════════════════════════════════════════════════════════════════
print("\n📦 SECTION 6: Google Sheets Export")


@test("GS-001: Google credentials load successfully")
def test_gs_creds():
    from domain.integrations.gsheet_exporter import get_google_creds

    creds = get_google_creds()
    assert creds is not None
    assert hasattr(creds, "service_account_email")


@test("GS-002: GSHEET_ID is configured")
def test_gs_id():
    gid = os.getenv("GSHEET_ID") or os.getenv("GOOGLE_SHEET_ID")
    assert gid and gid.strip(), "GSHEET_ID / GOOGLE_SHEET_ID not set in environment"


@test("GS-003: Sheet is accessible (live)")
def test_gs_access():
    if not os.getenv("RUN_GSHEET_E2E"):
        raise SkipTest("Set RUN_GSHEET_E2E=1 to open the spreadsheet via API")

    import gspread

    from domain.integrations.gsheet_exporter import get_google_creds

    gid = os.getenv("GSHEET_ID") or os.getenv("GOOGLE_SHEET_ID")
    creds = get_google_creds()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(gid.strip())
    assert sh is not None
    assert sh.title


@test("GS-004: Export dummy row to sheet (live)")
def test_gs_export():
    if not os.getenv("RUN_GSHEET_E2E"):
        raise SkipTest("Set RUN_GSHEET_E2E=1 to run live export")

    from domain.integrations.gsheet_exporter import export_to_gsheet

    reviews = [
        {
            "account_name": "TEST - Delete Me",
            "account_id": "001000000000000AAA",
            "opportunity_id": "006000000000000AAA",
            "opp": {
                "Id": "006000000000000AAA",
                "Name": "Test Renewal",
                "CloseDate": "2027-01-31",
                "StageName": "01 Initiate",
                "Forecasted_Attrition__c": 500000,
                "Swing__c": 100000,
                "License_At_Risk_Reason__c": "Test Reason",
                "ACV_Reason_Detail__c": "",
                "NextStep": "Test Next Step",
                "Specialist_Sales_Notes__c": "Test Notes",
                "Manager_Forecast_Judgement__c": "Best Case",
                "Description": "",
            },
            "snowflake_display": {
                "ari_category": "High",
                "ari_probability": "77.3%",
                "ari_reason": "Financial & Contractual",
                "health_score": 63,
                "health_literal": "Yellow",
                "health_display": "Yellow (63)",
                "cc_aov": "$1.6M",
                "utilization_rate": "87.2%",
                "util_emoji": ":large_green_circle:",
            },
            "enrichment": {
                "renewal_aov": {
                    "renewal_aov": 1608311,
                    "renewal_atr_snow": 810000,
                    "csg_territory": "AMER REG",
                    "csg_geo": "EMEA",
                }
            },
            "red_account": {
                "Stage__c": "Precautionary",
                "Days_Red__c": 17,
                "Latest_Updates__c": "Test update",
            },
            "product_attrition": [],
            "all_products_attrition": [
                {"APM_LVL_1": "Commerce"},
                {"APM_LVL_1": "Sales"},
            ],
            "risk_notes": "- Test risk note 1\n- Test risk note 2",
            "recommendation": "- Test rec 1\n- Test rec 2",
            "adoption_pov": (
                "- B2C Commerce: 300.00M provisioned, 87% utilized (261.71M used)"
            ),
            "team": {"ae": "Test AE", "renewal_mgr": "Test RM", "csm": "Test CSM"},
        }
    ]
    url = export_to_gsheet(reviews, sheet_name="TEST - Delete Me")
    assert url, "Export returned empty URL"
    assert "spreadsheets" in url


@test("GS-005: HEADERS_22 has expected column count")
def test_gs_headers():
    from domain.integrations.gsheet_exporter import HEADERS_22

    assert len(HEADERS_22) == 24, f"Expected 24 headers, got {len(HEADERS_22)}"


for fn in [
    test_gs_creds,
    test_gs_id,
    test_gs_access,
    test_gs_export,
    test_gs_headers,
]:
    fn()

# ══════════════════════════════════════════════════════════════════════
# SECTION 7: AT-RISK LIST (/attrition-risk list view)
# ══════════════════════════════════════════════════════════════════════
print("\n📦 SECTION 7: At-Risk List")


@test("AR-001: Get at-risk accounts Commerce Cloud")
def test_at_risk_commerce():
    from domain.analytics.snowflake_client import get_at_risk_accounts_snowflake

    rows = get_at_risk_accounts_snowflake(cloud="Commerce Cloud", limit=10)
    assert len(rows) > 0, "No at-risk accounts found"
    assert rows[0].get("ACCOUNT_ID") or rows[0].get("account_id"), "No account ID"
    cat = rows[0].get("ATTRITION_PROBA_CATEGORY") or rows[0].get(
        "attrition_proba_category"
    )
    assert cat in ("High", "Medium", "Low"), f"Unexpected ARI category: {cat!r}"


@test("AR-002: Get at-risk accounts with High ARI filter")
def test_at_risk_high():
    from domain.analytics.snowflake_client import get_at_risk_accounts_snowflake

    rows = get_at_risk_accounts_snowflake(
        cloud="Commerce Cloud",
        ari_filter="High",
        limit=10,
    )
    assert len(rows) > 0
    for r in rows:
        c = r.get("ATTRITION_PROBA_CATEGORY") or r.get("attrition_proba_category")
        assert c == "High"


@test("AR-003: Get at-risk accounts with ATR threshold")
def test_at_risk_atr():
    from domain.analytics.snowflake_client import get_at_risk_accounts_snowflake

    rows = get_at_risk_accounts_snowflake(
        cloud="Commerce Cloud",
        min_attrition=500000,
        limit=10,
    )
    for r in rows:
        pipe = r.get("ATTRITION_PIPELINE")
        if pipe is None:
            pipe = r.get("attrition_pipeline")
        assert abs(float(pipe or 0)) > 500000, (
            f"Pipeline not above threshold: {pipe!r}"
        )


@test("AR-004: Success Plans excluded from at-risk list")
def test_at_risk_no_success_plans():
    from domain.analytics.snowflake_client import get_at_risk_accounts_snowflake

    rows = get_at_risk_accounts_snowflake(cloud="Commerce Cloud", limit=25)
    for r in rows:
        l2 = str(r.get("APM_LVL_2") or r.get("apm_lvl_2") or "").lower()
        l3 = str(r.get("APM_LVL_3") or r.get("apm_lvl_3") or "").lower()
        assert "success plan" not in l2 and "success plan" not in l3, (
            f"Success Plan in list: {r.get('APM_LVL_2') or r.get('apm_lvl_2')}"
        )


@test("AR-005: SF Products display (cleaned names)")
def test_sf_products():
    from domain.analytics.snowflake_client import get_sf_products_display

    products = [
        {"APM_LVL_1": "Commerce"},
        {"APM_LVL_1": "Salesforce Platform"},
        {"APM_LVL_1": "Integration"},
        {"APM_LVL_1": "Other"},
        {"APM_LVL_1": "Sales"},
    ]
    result = get_sf_products_display(products)
    assert "Platform" in result, "Platform not mapped"
    assert "MuleSoft" in result, "Integration not mapped to MuleSoft"
    assert "Other" not in result, "Other should be excluded"
    assert "Salesforce Platform" not in result, "Should be shortened to Platform"


for fn in [
    test_at_risk_commerce,
    test_at_risk_high,
    test_at_risk_atr,
    test_at_risk_no_success_plans,
    test_sf_products,
]:
    fn()

# ══════════════════════════════════════════════════════════════════════
# SECTION 8: EDGE CASES & RESILIENCE
# ══════════════════════════════════════════════════════════════════════
print("\n📦 SECTION 8: Edge Cases & Resilience")


@test("EC-001: Account with no renewal opp handles gracefully")
def test_no_renewal_opp():
    from domain.analytics.snowflake_client import enrich_account, format_enrichment_for_display

    enrichment = enrich_account("001000000FAKE000AAA", None, "Commerce Cloud")
    display = format_enrichment_for_display(enrichment)
    assert display.get("ari_category") is not None
    assert display.get("health_display") is not None


@test("EC-002: 15-char vs 18-char ID conversion")
def test_id_conversion():
    from domain.analytics.snowflake_client import to_15_char_id

    id_18 = "0063y00001ANfq2AAD"
    id_15 = to_15_char_id(id_18)
    assert len(id_15) == 15
    assert id_15 == "0063y00001ANfq2"
    assert to_15_char_id(id_15) == id_15


@test("EC-003: HTML cleaned from Latest Updates")
def test_html_clean():
    from domain.salesforce.org62_client import clean_html

    html = "<p><strong>Mar-10:</strong> Some update</p>"
    cleaned = clean_html(html)
    assert "<p>" not in cleaned
    assert "<strong>" not in cleaned
    assert "Mar-10:" in cleaned


@test("EC-004: fmt_amount handles None and zero")
def test_fmt_amount_edge():
    from domain.analytics.snowflake_client import fmt_amount

    assert fmt_amount(None) == "N/A"
    assert fmt_amount(0) == "$0"
    assert fmt_amount("") == "N/A"


@test("EC-005: Snowflake pool re-inits after reset")
def test_snow_reconnect():
    from domain.analytics.snowflake_client import (
        get_snowflake_connection,
        reset_snowflake_pool,
        return_connection,
    )

    reset_snowflake_pool()
    conn = get_snowflake_connection()
    try:
        assert conn is not None
        assert not conn.is_closed()
    finally:
        return_connection(conn)


@test("EC-006: is_success_plan identifies correctly")
def test_is_success_plan():
    from domain.analytics.snowflake_client import is_success_plan

    assert is_success_plan({"APM_LVL_3": "Commerce - Success Plans - Premier"})
    assert is_success_plan({"APM_LVL_2": "Commerce - Success Plans - Signature"})
    assert not is_success_plan({"APM_LVL_3": "B2C Commerce (B2Ce)"})
    assert not is_success_plan({"APM_LVL_3": "Order Management"})


@test("EC-007: Cloud input parsing with comma-separated names")
def test_cloud_input_parsing():
    from filter_parser import CLOUD_KEYWORDS, parse_filters

    text = "Commerce Cloud, Adidas AG, Oxford Industries"
    parts = [p.strip() for p in text.split(",")]
    cloud_lower = {kw.lower() for kw in CLOUD_KEYWORDS}
    inputs = [p for p in parts if p.lower() not in cloud_lower]
    assert len(inputs) == 2
    assert "Adidas AG" in inputs
    assert "Oxford Industries" in inputs
    f = parse_filters(text)
    assert f["cloud"] == "Commerce Cloud"


for fn in [
    test_no_renewal_opp,
    test_id_conversion,
    test_html_clean,
    test_fmt_amount_edge,
    test_snow_reconnect,
    test_is_success_plan,
    test_cloud_input_parsing,
]:
    fn()

# ══════════════════════════════════════════════════════════════════════
# FINAL RESULTS
# ══════════════════════════════════════════════════════════════════════
elapsed = (datetime.now() - results["start_time"]).total_seconds()

print("\n" + "=" * 70)
print("TEST RESULTS SUMMARY")
print("=" * 70)
print(f"  ✅ PASSED:  {len(results['passed'])}")
print(f"  ❌ FAILED:  {len(results['failed'])}")
print(f"  ⏭️  SKIPPED: {len(results['skipped'])}")
print(f"  ⏱️  TIME:    {elapsed:.1f}s")
print()

if results["failed"]:
    print("FAILED TESTS:")
    for name, error in results["failed"]:
        print(f"  ❌ {name}")
        print(f"     {error[:100]}")
    print()

total = len(results["passed"]) + len(results["failed"])
pct = (len(results["passed"]) / total * 100) if total > 0 else 0
print(f"  {pct:.0f}% tests passing ({len(results['passed'])}/{total})")

if len(results["failed"]) == 0:
    print("\n  🎉 ALL TESTS PASSED!")
else:
    print(f"\n  ⚠️  {len(results['failed'])} test(s) need attention")

print("=" * 70)

sys.exit(1 if results["failed"] else 0)
