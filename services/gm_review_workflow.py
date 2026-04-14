"""
services/gm_review_workflow.py
GM Review orchestration — direct domain calls, no adapters.
"""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from domain.tracking.account_tracker import is_strategic, upsert_tracking
from log_utils import log_debug, log_structured

_OPP_ID_RE = re.compile(r"^006[A-Za-z0-9]{12,18}$")


def _gm_review_enrich_timeout_s() -> int:
    """Wall clock for ``enrich_account`` (health + usage + renewals + ARI in parallel)."""
    try:
        return max(45, int(os.getenv("GM_REVIEW_ENRICH_TIMEOUT", "120")))
    except ValueError:
        return 120


def _gm_review_attrition_all_timeout_s() -> int:
    """Timeout for ``get_account_attrition_all`` in parallel with enrich."""
    try:
        return max(20, int(os.getenv("GM_REVIEW_ATTRITION_ALL_TIMEOUT", "60")))
    except ValueError:
        return 60


def gm_review_max_concurrent_from_env() -> int:
    """
    Snowflake-friendly default is 1 (one account at a time). Set
    ``GM_REVIEW_MAX_CONCURRENT`` to 2–12 only if the warehouse and pool can absorb parallel bursts.
    """
    try:
        return max(1, min(12, int(os.getenv("GM_REVIEW_MAX_CONCURRENT", "1"))))
    except ValueError:
        return 1


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
    """Single Opportunity row — dynamic $ / notes from org62 (DYNAMIC_OPP_FIELDS)."""
    from domain.salesforce.org62_client import get_opportunity_by_id

    try:
        return get_opportunity_by_id(opp_id)
    except Exception as e:
        log_debug(f"_fetch_opportunity_record: {str(e)[:80]}")
        return None


def _auto_track_opp(
    account: dict,
    canvas_url: str | None = None,
    batch_id: str | None = None,
) -> None:
    """
    Auto-add strategic opps to tracking after GM review generation.
    Idempotent — safe to call on every run.
    """
    opp = account.get("opp") or {}
    disp = account.get("snowflake_display") or {}

    opp_payload = {
        "opp_id": opp.get("Id") or account.get("opp_id"),
        "account_id": account.get("account_id"),
        "account_name": account.get("account_name"),
        "cloud": account.get("cloud"),
        "ari_category": disp.get("ari_category") or account.get("ari_category"),
        "ari_probability": disp.get("ari_probability") or account.get("ari_probability"),
        "atr": (
            opp.get("Forecasted_Attrition__c")
            or (account.get("enrichment") or {}).get("renewal_aov", {}).get("renewal_atr_snow")
            or (account.get("enrichment") or {}).get("renewal_aov", {}).get("renewal_atr")
            or account.get("atr")
            or opp.get("Amount")
        ),
        "opp_stage": opp.get("StageName") or account.get("opp_stage"),
        "close_date": opp.get("CloseDate") or account.get("close_date"),
        "is_closed": opp.get("IsClosed") or account.get("is_closed"),
    }

    if not opp_payload.get("opp_id"):
        log_debug(
            f"No opp_id for {account.get('account_name', 'Unknown')} — skipping auto-track"
        )
        return

    try:
        if is_strategic(opp_payload):
            inserted = upsert_tracking(
                opp_payload,
                canvas_url=canvas_url,
                gm_review_batch=batch_id,
            )
            if inserted:
                log_debug(
                    f"✓ Auto-tracked strategic opp {opp_payload['opp_id']} "
                    f"({account.get('account_name')})"
                )
            else:
                log_debug(
                    f"✓ Updated tracking for opp {opp_payload['opp_id']} "
                    f"({account.get('account_name')})"
                )
        else:
            log_debug(f"Opp {opp_payload.get('opp_id')} not strategic — not tracked")
    except Exception as e:
        log_debug(f"_auto_track_opp error: {str(e)[:100]}")


class GMReviewWorkflow:
    """
    Lightweight GM Review workflow.
    No adapter layer — calls domain functions directly.

    By default runs accounts **sequentially** (``GM_REVIEW_MAX_CONCURRENT=1``) to avoid
    Snowflake stampedes; raise the env var only when the warehouse can handle it.
    """

    def __init__(self, call_llm_fn, max_concurrent: int | None = None):
        self.call_llm_fn = call_llm_fn
        self.max_concurrent = (
            max_concurrent
            if max_concurrent is not None
            else gm_review_max_concurrent_from_env()
        )

    def run(
        self,
        account_inputs: list,
        cloud: str = "Commerce Cloud",
        filter_label: str = "",
        today: str = "",
    ) -> dict[str, Any]:
        """
        Process multiple accounts (sequential when ``max_concurrent`` is 1, else parallel).

        Returns ``{"reviews": [...], "combined_canvas": markdown}`` for Slack/MCP parity.
        """
        from datetime import date

        from domain.analytics.snowflake_client import (
            clear_usage_snapshot_cache,
            enrich_account_cached,
            resolve_account_from_snowflake_cached,
        )
        from domain.content.canvas_builder import build_gm_review_canvas_markdown
        from domain.salesforce.org62_client import resolve_account_enhanced

        clear_usage_snapshot_cache()
        log_debug(f"GMReviewWorkflow: {len(account_inputs)} inputs, cloud={cloud}")
        run_id = str(uuid.uuid4())

        _resolution_cache: dict[str, dict] = {}
        _enrichment_cache: dict[tuple, dict] = {}

        def _resolve_with_cache(name: str) -> dict | None:
            key = name.strip().lower()
            if key in _resolution_cache:
                log_debug(f"Cache hit for account: {name}")
                return _resolution_cache[key]
            result = resolve_account_enhanced(name, cloud=cloud)
            if not result:
                snow = resolve_account_from_snowflake_cached(name, cloud=cloud)
                if snow:
                    result = {
                        "id": snow.get("account_id"),
                        "name": snow.get("account_name"),
                        "opty_id": snow.get("opty_id") or "",
                        "renewal_prefetch": {
                            "renewal_aov": snow.get("renewal_aov"),
                            "renewal_atr_snow": snow.get("renewal_atr_snow"),
                            "csg_territory": snow.get("csg_territory") or "",
                            "csg_area": snow.get("csg_area") or "",
                            "csg_geo": snow.get("csg_geo") or "",
                            "target_cloud": snow.get("target_cloud") or "",
                        },
                    }
            if result:
                _resolution_cache[key] = result
            return result

        def _enrich_with_cache(
            account_id_15: str,
            opty_id=None,
            cloud=cloud,
            usage_account_ids=None,
            renewal_prefetch=None,
        ) -> dict:
            key = (
                account_id_15.strip().lower(),
                (opty_id or "") or "",
                json.dumps(usage_account_ids or [], sort_keys=True, default=str),
                json.dumps(renewal_prefetch or {}, sort_keys=True, default=str),
            )
            if key in _enrichment_cache:
                log_debug(f"Enrichment cache hit for account_id: {account_id_15}")
                return _enrichment_cache[key]
            result = enrich_account_cached(
                account_id_15,
                opty_id=opty_id,
                cloud=cloud,
                usage_account_ids=usage_account_ids,
                renewal_prefetch=renewal_prefetch,
            )
            if result:
                _enrichment_cache[key] = result
            return result

        reviews: list = []
        if self.max_concurrent <= 1:
            log_debug(
                f"GMReviewWorkflow: sequential run ({len(account_inputs)} inputs, "
                f"max_concurrent=1)"
            )
            for inp in account_inputs:
                try:
                    result = self._generate_review(
                        inp,
                        cloud,
                        _resolve_with_cache,
                        _enrich_with_cache,
                        run_id,
                    )
                    if result:
                        reviews.append(result)
                        log_debug(f"✓ {result.get('account_name', inp)}")
                except Exception as e:
                    log_debug(f"❌ {inp}: {str(e)[:100]}")
        else:
            log_debug(
                f"GMReviewWorkflow: parallel run ({len(account_inputs)} inputs, "
                f"max_concurrent={self.max_concurrent})"
            )
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
                for future in as_completed(futures, timeout=3600):
                    inp = futures[future]
                    try:
                        result = future.result(timeout=600)
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
                "opportunity_id": r.get("opportunity_id") or "",
                "cloud": r.get("cloud") or cloud,
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
            enrich_account_cached,
            filter_products_by_cloud,
            format_enrichment_for_display,
            get_account_attrition_all,
            get_open_renewal_from_snowflake,
            to_15_char_id,
        )
        from domain.content.canvas_builder import build_adoption_pov
        from domain.intelligence.risk_engine import generate_risk_analysis
        from domain.salesforce.org62_client import (
            get_account_hierarchy,
            get_account_team,
            get_red_account,
            get_renewal_opportunities,
            get_renewal_opportunities_any_cloud,
            resolve_account_enhanced,
        )

        t0 = time.time()
        raw_in = (account_input or "").strip()

        # Snowflake-first: open renewal row (excludes Closed/Dead; latest snapshot; top ACV).
        t_resolve_start = time.time()
        snow_opp = get_open_renewal_from_snowflake(raw_in, cloud=cloud)

        opp: dict = {}
        needs_renewal_lookup = False
        opty_id_from_snow = ""
        account_id = ""
        account_name = ""
        renewal_prefetch_for_enrich: dict | None = None

        if snow_opp:
            account_id = str(snow_opp.get("account_id") or "")
            account_name = " ".join(str(snow_opp.get("account_name") or "").split())
            opty_id_from_snow = str(snow_opp.get("opty_id") or "")
            if str(snow_opp.get("opty_id") or "").strip():
                # Same field names as get_renewal_aov(); skip ids handled at workflow level.
                renewal_prefetch_for_enrich = {
                    k: v
                    for k, v in snow_opp.items()
                    if k not in ("opty_id", "account_id")
                }
                renewal_prefetch_for_enrich["account_name"] = account_name
        elif _OPP_ID_RE.match(raw_in):
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
                ro = str(acct.get("opty_id") or "").strip()
                if ro:
                    opty_id_from_snow = ro
                if acct.get("renewal_prefetch"):
                    renewal_prefetch_for_enrich = acct["renewal_prefetch"]

        if not account_id:
            return None

        account_id_15 = to_15_char_id(account_id)
        usage_account_ids_15 = [account_id_15]
        try:
            hierarchy_ids = get_account_hierarchy(account_id) or []
            if hierarchy_ids:
                usage_account_ids_15 = [
                    to_15_char_id(str(aid)) for aid in hierarchy_ids if str(aid).strip()
                ]
                usage_account_ids_15 = list(dict.fromkeys(usage_account_ids_15))
                log_debug(
                    f"Hierarchy usage ids ({len(usage_account_ids_15)}): "
                    f"{', '.join(usage_account_ids_15)}"
                )
        except Exception as e:
            log_debug(f"get_account_hierarchy error: {str(e)[:80]}")
        t_resolve = time.time() - t_resolve_start
        log_debug(f"  [timing] resolve: {t_resolve:.2f}s")

        if opty_id_from_snow and not (opp or {}).get("Id"):
            rec = _fetch_opportunity_record(opty_id_from_snow)
            if rec:
                opp = rec

        red: dict = {}
        team: dict | list = {}

        t_sf_start = time.time()
        t_sf = 0.0
        if needs_renewal_lookup:
            if opty_id_from_snow:
                # Open renewal opty already chosen in Snowflake — skip SF opp listing
                with ThreadPoolExecutor(max_workers=2) as sf_ex:
                    fut_red = sf_ex.submit(get_red_account, account_id)
                    fut_team = sf_ex.submit(get_account_team, account_id)
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
                if not (opp or {}).get("Id"):
                    opp = {}
                t_sf = time.time() - t_sf_start
                log_debug(f"  [timing] SF parallel: {t_sf:.2f}s")
            else:
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
        if not opty_id and opty_id_from_snow:
            opty_id = opty_id_from_snow

        _enrich_fn = enrich_account_fn or enrich_account_cached

        t_snow_start = time.time()
        snow_note = "" if needs_renewal_lookup else " (incl. overlapped SF)"
        _heavy_hierarchy = len(usage_account_ids_15) > 1

        def _run_attrition() -> tuple[list, list]:
            try:
                attrition_data = get_account_attrition_all(account_id_15)
                all_p = attrition_data.get("all", [])
                return filter_products_by_cloud(all_p, cloud), all_p
            except Exception as e:
                log_debug(f"get_account_attrition_all error: {str(e)[:60]}")
                return [], []

        if needs_renewal_lookup:
            if _heavy_hierarchy:
                try:
                    enrichment = _enrich_fn(
                        account_id_15,
                        opty_id or None,
                        cloud,
                        usage_account_ids_15,
                        renewal_prefetch_for_enrich,
                    )
                except Exception as e:
                    log_debug(
                        f"enrich_account error: {type(e).__name__}: {str(e) or repr(e)}"
                    )
                    enrichment = {}
                product_attrition, all_products = _run_attrition()
            else:
                with ThreadPoolExecutor(max_workers=2) as ex:
                    fut_enrich = ex.submit(
                        _enrich_fn,
                        account_id_15,
                        opty_id or None,
                        cloud,
                        usage_account_ids_15,
                        renewal_prefetch_for_enrich,
                    )
                    fut_attrition = ex.submit(get_account_attrition_all, account_id_15)

                    try:
                        enrichment = fut_enrich.result(
                            timeout=_gm_review_enrich_timeout_s()
                        )
                    except Exception as e:
                        log_debug(
                            f"enrich_account error: {type(e).__name__}: "
                            f"{str(e) or repr(e)}"
                        )
                        enrichment = {}

                    try:
                        attrition_data = fut_attrition.result(
                            timeout=_gm_review_attrition_all_timeout_s()
                        )
                        all_products = attrition_data.get("all", [])
                        product_attrition = filter_products_by_cloud(
                            all_products, cloud
                        )
                    except Exception as e:
                        log_debug(f"get_account_attrition_all error: {str(e)[:60]}")
                        product_attrition = []
                        all_products = []

            t_snow = time.time() - t_snow_start
            log_debug(
                f"  [timing] Snowflake: {t_snow:.2f}s{snow_note}"
            )
        else:
            # Case 2: opp already known — enrich + attrition + red + team
            if _heavy_hierarchy:
                try:
                    enrichment = _enrich_fn(
                        account_id_15,
                        opty_id or None,
                        cloud,
                        usage_account_ids_15,
                        renewal_prefetch_for_enrich,
                    )
                except Exception as e:
                    log_debug(
                        f"enrich_account error: {type(e).__name__}: {str(e) or repr(e)}"
                    )
                    enrichment = {}
                products, all_products = _run_attrition()
                with ThreadPoolExecutor(max_workers=2) as ex:
                    fut_red = ex.submit(get_red_account, account_id)
                    fut_team = ex.submit(get_account_team, account_id)
                    try:
                        red = fut_red.result(timeout=15)
                    except Exception as e:
                        log_debug(f"get_red_account error: {str(e)[:60]}")
                        red = {}
                    try:
                        team = fut_team.result(timeout=15)
                    except Exception as e:
                        log_debug(f"get_account_team error: {str(e)[:60]}")
                        team = {}
                product_attrition = products
            else:
                with ThreadPoolExecutor(max_workers=4) as ex:
                    fut_enrich = ex.submit(
                        _enrich_fn,
                        account_id_15,
                        opty_id or None,
                        cloud,
                        usage_account_ids_15,
                        renewal_prefetch_for_enrich,
                    )
                    fut_attrition = ex.submit(get_account_attrition_all, account_id_15)
                    fut_red = ex.submit(get_red_account, account_id)
                    fut_team = ex.submit(get_account_team, account_id)

                    try:
                        enrichment = fut_enrich.result(
                            timeout=_gm_review_enrich_timeout_s()
                        )
                    except Exception as e:
                        log_debug(
                            f"enrich_account error: {type(e).__name__}: "
                            f"{str(e) or repr(e)}"
                        )
                        enrichment = {}

                    try:
                        attrition_data = fut_attrition.result(
                            timeout=_gm_review_attrition_all_timeout_s()
                        )
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

        auto_track_payload = {
            "opp_id": (opp or {}).get("Id") or opty_id_from_snow,
            "account_id": account_id,
            "account_name": account_name,
            "cloud": cloud,
            "opp": opp or {},
            "snowflake_display": display or {},
            "enrichment": enrichment or {},
            "atr": (
                (opp or {}).get("Forecasted_Attrition__c")
                or (enrichment.get("renewal_aov", {}) or {}).get("renewal_atr_snow")
                or (enrichment.get("renewal_aov", {}) or {}).get("renewal_atr")
            ),
            "opp_stage": (opp or {}).get("StageName"),
            "close_date": (opp or {}).get("CloseDate"),
            "is_closed": (opp or {}).get("IsClosed"),
        }
        _auto_track_opp(account=auto_track_payload, canvas_url=None, batch_id=run_id)

        return {
            "account_id": account_id,
            "account_name": account_name,
            "cloud": cloud,
            "opportunity_id": opty_id,
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
