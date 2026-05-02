"""
Bulk CIDM usage queries.
Single query returns usage data for ALL accounts at once.

Commerce Cloud ``cc_aov`` for GM bulk reviews should come from Blaze
(``CI_BLAZE_FACT_ACCOUNT_VW``.``CC_BEGIN_AOV`` summed per combo), **not**
``begin_aov_total`` (all-cloud) — see ``get_blaze_cc_aov_by_combo_bulk``.
"""
import os
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime

from log_utils import log_debug

from domain.analytics.snowflake_client import run_query

_BLAZE_FACT_VW = "SSE_DM_CSG_RPT_PRD.CIDM.CI_BLAZE_FACT_ACCOUNT_VW"
_USAGE_EXTRACT_VW = "SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW"


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except Exception:
        return default


def _snowflake_cell(row: dict, *keys: str, default=None):
    for k in keys:
        for variant in (k, k.upper() if k else k):
            if not variant:
                continue
            v = row.get(variant)
            if v is not None and v != "":
                return v
    return default


def get_blaze_cc_aov_by_combo_bulk(
    combo_company_ids: list[str],
) -> dict[str, float]:
    """
    Sum **Commerce Cloud** beginning AOV per ``COMBO_COMPANY_ID`` from Blaze facts.

    Uses ``CC_BEGIN_AOV`` (not ``begin_aov_total``) with ``CURR_SNAP = 'Y'``, matching CSG reporting.
    """
    if not combo_company_ids:
        return {}
    seen: set[str] = set()
    esc: list[str] = []
    for raw in combo_company_ids:
        c = str(raw or "").strip()
        if not c or c in seen:
            continue
        seen.add(c)
        esc.append(c.replace("'", "''"))
    if not esc:
        return {}
    ids_sql = "','".join(esc)
    sql = f"""
        SELECT
            blz.COMBO_COMPANY_ID AS COMBO_COMPANY_ID,
            SUM(COALESCE(blz.CC_BEGIN_AOV, 0)) AS CC_AOV
        FROM {_BLAZE_FACT_VW} blz
        WHERE blz.CURR_SNAP = 'Y'
          AND blz.COMBO_COMPANY_ID IN ('{ids_sql}')
        GROUP BY blz.COMBO_COMPANY_ID
    """
    try:
        rows = run_query(
            sql,
            [],
            statement_timeout=_env_int(
                "SNOWFLAKE_BLAZE_FACT_STATEMENT_TIMEOUT", 120
            ),
        )
    except Exception as e:
        log_debug(f"get_blaze_cc_aov_by_combo_bulk: {str(e)[:200]}")
        return {}
    out: dict[str, float] = {}
    for rw in rows or []:
        cid = str(_snowflake_cell(rw, "COMBO_COMPANY_ID", default="")).strip()
        if cid:
            out[cid] = float(_snowflake_cell(rw, "CC_AOV", default=0) or 0)
    return out


def _parse_contract_end(close_raw: str) -> date | None:
    """Renewal close anchor; YYYY-MM-DD or YYYY-MM (last day of month)."""
    s = (close_raw or "").strip()
    if not s:
        return None
    try:
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        key = s[:7]
        if len(key) == 7 and key[4] == "-":
            y, m = int(key[:4]), int(key[5:7])
            last = monthrange(y, m)[1]
            return date(y, m, last)
    except ValueError:
        return None
    return None


def _add_months(d: date, delta_months: int) -> date:
    m = d.month - 1 + delta_months
    y = d.year + m // 12
    m = m % 12 + 1
    day = min(d.day, monthrange(y, m)[1])
    return date(y, m, day)


def _coerce_sql_date(val) -> date | None:
    """Normalize Snowflake / connector date values."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()[:10]
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


def _commerce_burn_line_label(l2: str, grp: object) -> str:
    """Display label aligned with CIDM ``GRP`` + ``DRVD_APM_LVL_2`` row."""
    g = str(grp or "").strip().upper()
    l2s = str(l2 or "").strip()
    if g == "PPO":
        return "B2C PPO"
    if g == "GMV":
        return "B2C GMV"
    if "B2B" in l2s:
        return "B2B"
    if "Order Management" in l2s or "OMS" in l2s:
        return "OMS"
    return l2s if l2s else "Commerce"


def _merge_commerce_burn_by_label(rows: list[dict]) -> list[dict]:
    """Sum PROVISIONED / USED across SQL rows that map to the same display label."""
    totals: dict[str, dict[str, float]] = {}
    order: list[str] = []
    for rec in rows:
        lbl = _commerce_burn_line_label(rec["l2"], rec.get("grp"))
        if lbl not in totals:
            totals[lbl] = {"prov": 0.0, "used": 0.0}
            order.append(lbl)
        totals[lbl]["prov"] += float(rec["prov"] or 0)
        totals[lbl]["used"] += float(rec["used"] or 0)
    return [
        {"l2_label": lbl, "prov": totals[lbl]["prov"], "used": totals[lbl]["used"]}
        for lbl in order
    ]


def _burn_status(overall_burn: float) -> str:
    if overall_burn < 0.5:
        return "CRITICAL"
    if overall_burn < 0.7:
        return "HIGH RISK"
    if overall_burn < 0.9:
        return "AT RISK"
    return "HEALTHY"


def get_commerce_burn_rate_bulk(
    account_ids: list[str],
    contract_dates: dict[str, dict],
) -> dict[str, dict]:
    """
    Commerce-only usage burn metrics per account from ``WV_AV_USAGE_EXTRACT_VW``.
    Rows are constrained with ``CURR_SNAP_FLG = 'Y'`` only (no fixed ``SNAPSHOT_DT`` —
    snapshot month can differ by account). Sandbox credit rows are excluded
    (``GRP``, ``TYPE``). ``burn_rate_by_l2`` keys use display labels (B2C PPO,
    B2C GMV, B2B, OMS, …). Overall burn is weighted by provisioned across all
    non-Sandbox buckets.

    ``contract_dates`` maps 15-char ``ACCOUNT_ID`` to
    ``{"renewal_close_dt": str, "term_months": int, "contract_start_date": optional}``.
    When ``contract_start_date`` is set (from Org62 prior period start), it is used as
    contract start instead of ``renewal_close_dt`` minus ``term_months``.

    Returns per-account dict with ``burn_rate_overall``, ``predicted_eoc_util``,
    ``util_pct_overall``, ``time_elapsed_pct``, ``burn_rate_by_l2``, ``status``.
    """
    if not account_ids:
        return {}

    seen: set[str] = set()
    ids_esc: list[str] = []
    for aid in account_ids:
        a15 = str(aid or "").strip()[:15]
        if not a15 or a15 in seen:
            continue
        seen.add(a15)
        ids_esc.append(a15.replace("'", "''"))
    if not ids_esc:
        return {}

    ids_sql = "','".join(ids_esc)
    sql = f"""
        SELECT
            ACCOUNT_ID,
            DRVD_APM_LVL_2,
            GRP,
            MAX(SNAPSHOT_DT) AS SNAPSHOT_DT,
            SUM(COALESCE(PROVISIONED, 0)) AS PROV,
            SUM(COALESCE(USED, 0)) AS USED_SUM
        FROM {_USAGE_EXTRACT_VW}
        WHERE ACCOUNT_ID IN ('{ids_sql}')
          AND DRVD_APM_LVL_1 = 'Commerce'
          AND CURR_SNAP_FLG = 'Y'
          AND GRP != 'Sandbox'
          AND TYPE NOT LIKE '%Sandbox%'
        GROUP BY ACCOUNT_ID, DRVD_APM_LVL_2, GRP
    """
    try:
        rows = run_query(
            sql,
            [],
            statement_timeout=_env_int(
                "SNOWFLAKE_BLAZE_FACT_STATEMENT_TIMEOUT", 120
            ),
        )
    except Exception as e:
        log_debug(f"get_commerce_burn_rate_bulk: {str(e)[:200]}")
        return {}

    try:
        default_term = int(os.getenv("GM_REVIEW_COMMERCE_TERM_MONTHS", "12") or "12")
    except ValueError:
        default_term = 12
    default_term = max(default_term, 1)
    by_acct: dict[str, list[dict]] = defaultdict(list)
    for rw in rows or []:
        aid = str(_snowflake_cell(rw, "ACCOUNT_ID", default="")).strip()[:15]
        if not aid:
            continue
        l2 = str(_snowflake_cell(rw, "DRVD_APM_LVL_2", default="") or "").strip()
        if not l2:
            l2 = "Commerce"
        grp_raw = _snowflake_cell(rw, "GRP", default="")
        grp = str(grp_raw or "").strip()
        prov = float(_snowflake_cell(rw, "PROV", default=0) or 0)
        used = float(_snowflake_cell(rw, "USED_SUM", "USED", default=0) or 0)
        snap_d = _coerce_sql_date(_snowflake_cell(rw, "SNAPSHOT_DT"))
        by_acct[aid].append(
            {"l2": l2, "grp": grp, "prov": prov, "used": used, "snapshot_dt": snap_d}
        )

    out: dict[str, dict] = {}
    for acct, l2_rows in by_acct.items():
        spec = contract_dates.get(acct) or {}
        close_raw = str(spec.get("renewal_close_dt") or "").strip()
        term_m = int(spec.get("term_months") or default_term)
        contract_end = _parse_contract_end(close_raw)
        if contract_end is None or term_m <= 0:
            continue
        start_raw = spec.get("contract_start_date")
        if start_raw is not None and str(start_raw).strip() != "":
            contract_start = _parse_contract_end(str(start_raw).strip())
            if contract_start is None:
                contract_start = _add_months(contract_end, -term_m)
        else:
            contract_start = _add_months(contract_end, -term_m)
        total_days = max((contract_end - contract_start).days, 1)
        snap_dates = [
            rec["snapshot_dt"] for rec in l2_rows if rec.get("snapshot_dt")
        ]
        acct_snap = max(snap_dates) if snap_dates else date.today()
        ref = min(acct_snap, contract_end)
        if ref < contract_start:
            elapsed_days = 0
        else:
            elapsed_days = max((ref - contract_start).days, 0)
        time_elapsed_pct = (elapsed_days / total_days) * 100.0 if total_days else 0.0
        denom_elapsed = max(elapsed_days, 1)

        merged = _merge_commerce_burn_by_label(l2_rows)

        burn_rate_by_l2: dict[str, dict] = {}
        weights_burn: list[tuple[float, float]] = []
        weights_eoc: list[tuple[float, float]] = []
        total_prov_nonzero = 0.0
        total_used_nonzero = 0.0

        # Weighted burn across all non-Sandbox buckets by provisioned
        for rec in merged:
            l2_key = rec["l2_label"]
            prov = rec["prov"]
            used = rec["used"]
            if prov <= 0:
                continue
            util_pct = (used / prov) * 100.0
            if time_elapsed_pct > 0:
                burn_l2 = util_pct / time_elapsed_pct
            else:
                burn_l2 = 0.0
            predicted_eoc_l2 = (
                (used / denom_elapsed) * total_days / prov * 100.0
            )
            burn_rate_by_l2[l2_key] = {
                "prov": prov,
                "used": used,
                "util_pct": util_pct,
                "burn_rate": burn_l2,
                "predicted_eoc": predicted_eoc_l2,
            }
            weights_burn.append((burn_l2, prov))
            weights_eoc.append((predicted_eoc_l2, prov))
            total_prov_nonzero += prov
            total_used_nonzero += used

        if not burn_rate_by_l2:
            continue

        util_pct_overall = (
            (total_used_nonzero / total_prov_nonzero) * 100.0
            if total_prov_nonzero > 0
            else 0.0
        )
        wsum = sum(w for _, w in weights_burn)
        burn_rate_overall = (
            sum(b * w for b, w in weights_burn) / wsum if wsum > 0 else 0.0
        )
        wsum_e = sum(w for _, w in weights_eoc)
        predicted_eoc_util = (
            sum(e * w for e, w in weights_eoc) / wsum_e if wsum_e > 0 else 0.0
        )

        out[acct] = {
            "burn_rate_overall": burn_rate_overall,
            "predicted_eoc_util": predicted_eoc_util,
            "util_pct_overall": util_pct_overall,
            "time_elapsed_pct": time_elapsed_pct,
            "burn_rate_by_l2": burn_rate_by_l2,
            "status": _burn_status(burn_rate_overall),
        }

    return out


def get_usage_bulk(account_ids: list[str], cloud: str = None) -> dict:
    """
    Single CIDM query for all accounts.
    Rows use ``CURR_SNAP_FLG = 'Y'`` only so each account gets its latest snapshot row
    (snapshot date varies by account).

    Returns {account_id_15: usage_data} where usage_data includes:
      - utilization_rate
      - provisioned / used
      - products
      - raw_rows       (cloud-filtered, for Adoption POV)
      - all_raw_rows   (unfiltered, for SF Products)
    """
    if not account_ids:
        return {}

    ids_15 = [aid[:15] for aid in account_ids if aid]
    ids_sql = "','".join(ids_15)

    # Fetch ALL rows first (no cloud filter)
    all_rows = run_query(f"""
        SELECT
            ACCOUNT_ID,
            DRVD_APM_LVL_1,
            DRVD_APM_LVL_2,
            GRP,
            TYPE,
            PROVISIONED,
            ACTIVATED,
            USED
        FROM SSE_DM_CSG_RPT_PRD.CIDM.WV_AV_USAGE_EXTRACT_VW
        WHERE ACCOUNT_ID IN ('{ids_sql}')
        AND CURR_SNAP_FLG = 'Y'
        AND PROVISIONED > 0
    """)

    # Apply cloud filter in Python for filtered rows
    def _matches_cloud(row: dict, cloud_name: str) -> bool:
        if not cloud_name:
            return True
        c = cloud_name.lower()
        l1 = str(row.get("DRVD_APM_LVL_1") or "").lower()
        l2 = str(row.get("DRVD_APM_LVL_2") or "").lower()
        if "financial services" in c or c == "fsc":
            return "financial services" in l2 or "industries" in l1
        if "commerce" in c:
            return "commerce" in l1 or "commerce" in l2
        if "marketing" in c:
            return "marketing" in l1 or "marketing" in l2
        if "tableau" in c:
            return "tableau" in l1 or "tableau" in l2
        if "mulesoft" in c or "integration" in c:
            return "integration" in l1 or "mulesoft" in l1
        if "sales" in c:
            return "sales" in l1 or "sales" in l2
        if "service" in c:
            return "service" in l1 or "service" in l2
        return True

    # Group all rows by account
    all_account_rows = defaultdict(list)
    for r in all_rows:
        all_account_rows[r["ACCOUNT_ID"]].append(r)

    # Group cloud-filtered rows by account
    filtered_account_rows = defaultdict(list)
    for r in all_rows:
        if _matches_cloud(r, cloud):
            filtered_account_rows[r["ACCOUNT_ID"]].append(r)

    # Build usage map
    result = {}
    for acct_id, acct_all_rows in all_account_rows.items():
        filtered_rows = filtered_account_rows.get(acct_id, acct_all_rows)

        # Utilization from filtered rows (cloud-specific)
        total_prov = sum(float(r.get("PROVISIONED") or 0) for r in filtered_rows)
        total_used = sum(float(r.get("USED") or 0) for r in filtered_rows)
        util = (total_used / total_prov * 100) if total_prov > 0 else 0

        # SF Products from ALL rows (breadth view)
        apm_l1_all = list(dict.fromkeys(
            str(r.get("DRVD_APM_LVL_1") or "").strip()
            for r in acct_all_rows
            if str(r.get("DRVD_APM_LVL_1") or "").strip()
            and str(r.get("DRVD_APM_LVL_1") or "").strip() not in ("Other", "")
        ))

        result[acct_id] = {
            "utilization_rate": f"{util:.1f}%",
            "provisioned": total_prov,
            "used": total_used,
            "products": apm_l1_all,
            "sf_products": ", ".join(apm_l1_all),
            "raw_rows": filtered_rows,      # cloud-filtered -> Adoption POV
            "all_raw_rows": acct_all_rows,  # unfiltered -> SF Products
        }

    return result
