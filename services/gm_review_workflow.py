"""
services/gm_review_workflow.py
GM Review orchestration — direct domain calls, no adapters.
"""
from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from log_utils import log_debug

_OPP_ID_RE = re.compile(r"^006[A-Za-z0-9]{12,18}$")


def _resolve_open_opportunity_id(name: str) -> str | None:
    """First open Opportunity Id matching name (exact then LIKE)."""
    from domain.salesforce.org62_client import _escape, _soql_line, get_sf_client

    raw = (name or "").strip()
    if not raw:
        return None
    sf = get_sf_client()
    try:
        q = f"""
            SELECT Id FROM Opportunity
            WHERE Name = '{_escape(raw)}' AND IsClosed = false
            LIMIT 1
            """
        res = sf.query(_soql_line(q))
        if res.get("records"):
            return res["records"][0]["Id"]
        q2 = f"""
            SELECT Id FROM Opportunity
            WHERE Name LIKE '%{_escape(raw)}%' AND IsClosed = false
            ORDER BY CloseDate ASC
            LIMIT 1
            """
        res2 = sf.query(_soql_line(q2))
        if res2.get("records"):
            return res2["records"][0]["Id"]
    except Exception as e:
        log_debug(f"_resolve_open_opportunity_id: {str(e)[:80]}")
    return None


def _fetch_opportunity_record(opp_id: str) -> dict | None:
    """Single Opportunity row with Account sub-query fields (renewal SOQL shape)."""
    from domain.salesforce.org62_client import _escape, _soql_line, get_sf_client

    oid = _escape(opp_id)
    fields = (
        "Id, Name, StageName, Amount, CloseDate, "
        "Account.Id, Account.Name, Account.BillingCountry, "
        "ForecastCategoryName, Forecasted_Attrition__c, Swing__c, "
        "License_At_Risk_Reason__c, ACV_Reason_Detail__c, NextStep, "
        "Description, Specialist_Sales_Notes__c, "
        "Manager_Forecast_Judgement__c"
    )
    q = f"SELECT {fields} FROM Opportunity WHERE Id = '{oid}' LIMIT 1"
    sf = get_sf_client()
    try:
        result = sf.query(_soql_line(q))
        records = result.get("records", [])
        return records[0] if records else None
    except Exception as e:
        log_debug(f"_fetch_opportunity_record: {str(e)[:80]}")
        return None


class GMReviewWorkflow:
    """
    Lightweight GM Review workflow.
    No adapter layer — calls domain functions directly.
    """

    def __init__(self, call_llm_fn, max_concurrent: int = 5):
        self.call_llm_fn = call_llm_fn
        self.max_concurrent = max_concurrent

    def run(
        self,
        account_inputs: list,
        cloud: str = "Commerce Cloud",
        filter_label: str = "",
        today: str = "",
    ) -> dict[str, Any]:
        """
        Process multiple accounts in parallel.

        Returns ``{"reviews": [...], "combined_canvas": markdown}`` for Slack/MCP parity.
        """
        from datetime import date

        from domain.content.canvas_builder import build_gm_review_canvas_markdown

        log_debug(f"GMReviewWorkflow: {len(account_inputs)} inputs, cloud={cloud}")

        reviews: list = []
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            futures = {
                executor.submit(self._generate_review, inp, cloud): inp
                for inp in account_inputs
            }
            for future in as_completed(futures, timeout=120):
                inp = futures[future]
                try:
                    result = future.result(timeout=90)
                    if result:
                        reviews.append(result)
                        log_debug(f"✓ {result.get('account_name', inp)}")
                except Exception as e:
                    log_debug(f"❌ {inp}: {str(e)[:100]}")

        canvas_today = today or date.today().strftime("%A, %B %d, %Y")
        canvas_filter = filter_label or f"{cloud} - Q2 FY2027"
        canvas_reviews = [
            {
                "account_name": r["account_name"],
                "account_id": r["account_id"],
                "opp": r.get("opp") or {},
                "snowflake_display": r.get("snowflake_display") or {},
                "enrichment": r.get("enrichment") or {},
                "red_account": r.get("red_account"),
                "risk_notes": r.get("risk_notes", ""),
                "recommendation": r.get("recommendation", ""),
                "product_attrition": r.get("product_attrition") or [],
                "all_products_attrition": r.get("all_products_attrition") or [],
            }
            for r in reviews
        ]
        combined_canvas = build_gm_review_canvas_markdown(
            reviews=canvas_reviews,
            cloud=cloud,
            filter_label=canvas_filter,
            today=canvas_today,
        )

        return {"reviews": reviews, "combined_canvas": combined_canvas}

    def _generate_review(self, account_input: str, cloud: str) -> dict | None:
        """Generate a single account review — all direct domain calls."""
        from domain.analytics.snowflake_client import (
            enrich_account,
            format_enrichment_for_display,
            get_account_attrition,
            get_usage_raw_data,
            to_15_char_id,
        )
        from domain.content.canvas_builder import build_adoption_pov
        from domain.intelligence.risk_engine import generate_risk_analysis
        from domain.salesforce.org62_client import (
            get_account_team,
            get_red_account,
            get_renewal_opportunities,
            get_renewal_opportunities_any_cloud,
            resolve_account_enhanced,
        )

        start = time.time()
        raw_in = (account_input or "").strip()

        opp: dict = {}
        if _OPP_ID_RE.match(raw_in):
            rec = _fetch_opportunity_record(raw_in)
            if not rec:
                return None
            opp = rec
            acct_data = opp.get("Account") or {}
            account_id = str(acct_data.get("Id") or "")
            account_name = " ".join((acct_data.get("Name") or "Unknown").split())
        else:
            opp_id = _resolve_open_opportunity_id(raw_in)
            if opp_id:
                rec = _fetch_opportunity_record(opp_id)
                if not rec:
                    return None
                opp = rec
                acct_data = opp.get("Account") or {}
                account_id = str(acct_data.get("Id") or "")
                account_name = " ".join((acct_data.get("Name") or "Unknown").split())
            else:
                acct = resolve_account_enhanced(raw_in, cloud=cloud)
                if not acct:
                    log_debug(f"⚠️ Could not resolve: {raw_in}")
                    return None
                account_id = str(acct["id"])
                account_name = " ".join(str(acct["name"]).split())
                opps = get_renewal_opportunities(account_id, cloud)
                if not opps:
                    opps = get_renewal_opportunities_any_cloud(account_id)
                opp = opps[0] if opps else {}

        if not account_id:
            return None

        account_id_15 = to_15_char_id(account_id)
        opty_id = str(opp.get("Id", "") or "") if opp else ""

        with ThreadPoolExecutor(max_workers=4) as ex:
            fut_enrich = ex.submit(enrich_account, account_id, opty_id or None, cloud)
            fut_products = ex.submit(get_account_attrition, account_id_15, cloud)
            fut_all_prod = ex.submit(get_account_attrition, account_id_15, None)
            fut_usage = ex.submit(get_usage_raw_data, account_id_15, cloud)

            enrichment = fut_enrich.result()
            product_attrition = fut_products.result()
            all_products = fut_all_prod.result()
            usage_raw = fut_usage.result()

        display = format_enrichment_for_display(enrichment)
        adoption_pov = build_adoption_pov(usage_raw, cloud=cloud)

        red = get_red_account(account_id)
        team: dict = {}
        try:
            team = get_account_team(account_id) or {}
        except Exception:
            pass

        risk_notes, recommendation = generate_risk_analysis(
            account_name=account_name,
            opp=opp,
            red_account=red,
            snowflake_enrichment=enrichment,
            call_llm_fn=self.call_llm_fn,
        )

        log_debug(f"✓ {account_name} done in {time.time() - start:.1f}s")

        return {
            "account_id": account_id,
            "account_name": account_name,
            "opp": opp,
            "enrichment": enrichment,
            "snowflake_display": display,
            "product_attrition": product_attrition or [],
            "all_products_attrition": all_products or [],
            "red_account": red,
            "team": team,
            "risk_notes": risk_notes,
            "recommendation": recommendation,
            "adoption_pov": adoption_pov,
            "usage_raw": usage_raw,
        }
