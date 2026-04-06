"""
services/gm_review_workflow.py
GM Review orchestration — direct domain calls, no adapters.
"""
from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from log_utils import log_debug, log_structured

_OPP_ID_RE = re.compile(r"^006[A-Za-z0-9]{12,18}$")


def _resolve_open_opportunity_id(name: str) -> str | None:
    """First open Opportunity Id matching name (exact then LIKE)."""
    from domain.salesforce.org62_client import _escape, sf_query

    raw = (name or "").strip()
    if not raw:
        return None
    try:
        q = f"""
            SELECT Id FROM Opportunity
            WHERE Name = '{_escape(raw)}' AND IsClosed = false
            LIMIT 1
            """
        res = sf_query(q)
        if res.get("records"):
            return res["records"][0]["Id"]
        q2 = f"""
            SELECT Id FROM Opportunity
            WHERE Name LIKE '%{_escape(raw)}%' AND IsClosed = false
            ORDER BY CloseDate ASC
            LIMIT 1
            """
        res2 = sf_query(q2)
        if res2.get("records"):
            return res2["records"][0]["Id"]
    except Exception as e:
        log_debug(f"_resolve_open_opportunity_id: {str(e)[:80]}")
    return None


def _fetch_opportunity_record(opp_id: str) -> dict | None:
    """Single Opportunity row with Account sub-query fields (renewal SOQL shape)."""
    from domain.salesforce.org62_client import _escape, sf_query

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
    try:
        result = sf_query(q)
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

    def __init__(self, call_llm_fn, max_concurrent: int = 8):
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

        from domain.analytics.snowflake_client import enrich_account
        from domain.content.canvas_builder import build_gm_review_canvas_markdown
        from domain.salesforce.org62_client import resolve_account_enhanced

        log_debug(f"GMReviewWorkflow: {len(account_inputs)} inputs, cloud={cloud}")
        run_id = str(uuid.uuid4())

        _resolution_cache: dict[str, dict] = {}
        _enrichment_cache: dict[str, dict] = {}

        def _resolve_with_cache(name: str) -> dict | None:
            key = name.strip().lower()
            if key in _resolution_cache:
                log_debug(f"Cache hit for account: {name}")
                return _resolution_cache[key]
            result = resolve_account_enhanced(name, cloud=cloud)
            if result:
                _resolution_cache[key] = result
            return result

        def _enrich_with_cache(account_id_15: str, opty_id=None, cloud=cloud) -> dict:
            key = account_id_15.strip().lower()
            if key in _enrichment_cache:
                log_debug(f"Enrichment cache hit for account_id: {account_id_15}")
                return _enrichment_cache[key]
            result = enrich_account(account_id_15, opty_id=opty_id, cloud=cloud)
            if result:
                _enrichment_cache[key] = result
            return result

        reviews: list = []
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            futures = {
                executor.submit(
                    self._generate_review,
                    inp,
                    cloud,
                    _resolve_with_cache,
                    _enrich_with_cache,
                    run_id,
                ): inp
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

    def _generate_review(
        self,
        account_input: str,
        cloud: str,
        resolve_account_fn: Callable[[str], dict | None] | None = None,
        enrich_account_fn: Callable[..., dict] | None = None,
        run_id: str | None = None,
    ) -> dict | None:
        """Generate a single account review — all direct domain calls."""
        from domain.analytics.snowflake_client import (
            enrich_account,
            filter_products_by_cloud,
            format_enrichment_for_display,
            get_account_attrition_all,
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

        t0 = time.time()
        t_resolve_start = t0
        raw_in = (account_input or "").strip()

        opp: dict = {}
        needs_renewal_lookup = False
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
                if resolve_account_fn is not None:
                    acct = resolve_account_fn(raw_in)
                else:
                    acct = resolve_account_enhanced(raw_in, cloud=cloud)
                if not acct:
                    log_debug(f"⚠️ Could not resolve: {raw_in}")
                    return None
                account_id = str(acct["id"])
                account_name = " ".join(str(acct["name"]).split())
                needs_renewal_lookup = True

        if not account_id:
            return None

        account_id_15 = to_15_char_id(account_id)
        t_resolve = time.time() - t_resolve_start
        log_debug(f"  [timing] resolve: {t_resolve:.2f}s")

        red: dict = {}
        team: dict | list = {}

        t_sf_start = time.time()
        t_sf = 0.0
        if needs_renewal_lookup:
            # Case 1: account resolved by name — parallel SF before Snowflake
            with ThreadPoolExecutor(max_workers=3) as sf_ex:
                fut_opps = sf_ex.submit(get_renewal_opportunities, account_id, cloud)
                fut_red = sf_ex.submit(get_red_account, account_id)
                fut_team = sf_ex.submit(get_account_team, account_id)

                try:
                    opps = fut_opps.result(timeout=15)
                except Exception as e:
                    log_debug(f"get_renewal_opportunities error: {str(e)[:60]}")
                    opps = []

                try:
                    red = fut_red.result(timeout=15)
                except Exception as e:
                    log_debug(f"get_red_account error: {str(e)[:60]}")
                    red = {}

                try:
                    team = fut_team.result(timeout=15)
                except Exception as e:
                    log_debug(f"get_account_team error: {str(e)[:60]}")
                    team = []

            if not opps:
                opps = get_renewal_opportunities_any_cloud(account_id)
            if not opps:
                opps = []
            opp = opps[0] if opps else {}
            t_sf = time.time() - t_sf_start
            log_debug(f"  [timing] SF parallel: {t_sf:.2f}s")
        else:
            log_debug(
                "  [timing] SF parallel: 0.00s (overlapped with Snowflake pool)"
            )

        opty_id = str(opp.get("Id", "") or "") if opp else ""

        _enrich_fn = enrich_account_fn or enrich_account

        t_snow_start = time.time()
        snow_note = "" if needs_renewal_lookup else " (incl. overlapped SF)"
        if needs_renewal_lookup:
            with ThreadPoolExecutor(max_workers=2) as ex:
                fut_enrich = ex.submit(
                    _enrich_fn, account_id_15, opty_id or None, cloud
                )
                fut_attrition = ex.submit(get_account_attrition_all, account_id_15)

                try:
                    enrichment = fut_enrich.result(timeout=30)
                except Exception as e:
                    log_debug(f"enrich_account error: {str(e)[:60]}")
                    enrichment = {}

                try:
                    attrition_data = fut_attrition.result(timeout=20)
                    all_products = attrition_data.get("all", [])
                    product_attrition = filter_products_by_cloud(all_products, cloud)
                except Exception as e:
                    log_debug(f"get_account_attrition_all error: {str(e)[:60]}")
                    product_attrition = []
                    all_products = []

            t_snow = time.time() - t_snow_start
            log_debug(
                f"  [timing] Snowflake: {t_snow:.2f}s{snow_note}"
            )
        else:
            # Case 2: opp already known — enrich + attrition + red + team in one pool
            with ThreadPoolExecutor(max_workers=4) as ex:
                fut_enrich = ex.submit(
                    _enrich_fn, account_id_15, opty_id or None, cloud
                )
                fut_attrition = ex.submit(get_account_attrition_all, account_id_15)
                fut_red = ex.submit(get_red_account, account_id)
                fut_team = ex.submit(get_account_team, account_id)

                try:
                    enrichment = fut_enrich.result(timeout=30)
                except Exception as e:
                    log_debug(f"enrich_account error: {str(e)[:60]}")
                    enrichment = {}

                try:
                    attrition_data = fut_attrition.result(timeout=20)
                    all_products = attrition_data.get("all", [])
                    products = filter_products_by_cloud(all_products, cloud)
                except Exception as e:
                    log_debug(f"get_account_attrition_all error: {str(e)[:60]}")
                    products = []
                    all_products = []

                try:
                    red = fut_red.result(timeout=15)
                except Exception as e:
                    log_debug(f"get_red_account error: {str(e)[:60]}")
                    red = {}

                try:
                    team = fut_team.result(timeout=15)
                except Exception as e:
                    log_debug(f"get_account_team error: {str(e)[:60]}")
                    team = []

            product_attrition = products
            t_snow = time.time() - t_snow_start
            log_debug(
                f"  [timing] Snowflake: {t_snow:.2f}s{snow_note}"
            )

        if not isinstance(enrichment, dict):
            enrichment = {}
        usage_raw = enrichment.get("usage_raw_rows", [])

        display = format_enrichment_for_display(enrichment)
        adoption_pov = build_adoption_pov(usage_raw, cloud=cloud)

        t_llm_start = time.time()
        risk_notes, recommendation = generate_risk_analysis(
            account_name=account_name,
            opp=opp,
            red_account=red,
            snowflake_enrichment=enrichment,
            call_llm_fn=self.call_llm_fn,
        )
        t_llm = time.time() - t_llm_start
        log_debug(f"  [timing] LLM: {t_llm:.2f}s")
        log_debug(f"  [timing] total: {time.time() - t0:.2f}s — {account_name}")
        log_structured(
            "account_review",
            account=account_name,
            run_id=run_id or "",
            resolve_ms=round(t_resolve * 1000),
            sf_ms=round(t_sf * 1000),
            snowflake_ms=round(t_snow * 1000),
            llm_ms=round(t_llm * 1000),
            total_ms=round((time.time() - t0) * 1000),
            status="ok",
        )

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
