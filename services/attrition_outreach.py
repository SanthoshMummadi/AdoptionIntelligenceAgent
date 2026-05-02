"""
Commerce **attrition outreach** for GM Review sheet rows (module: ``attrition_outreach``).

Triggered when Col AB (Outreach Status) is exactly *Outreach Initiated* (not the checkmark).
After success the cell becomes *Outreach Initiated ✓*; restoring *Outreach Initiated* re-runs
outreach (same protect channel, new messages, new ``outreach_log`` row).

Batch scans run the protect-channel phase in parallel (see ``GM_REVIEW_STAGE3_MAX_WORKERS``),
then sheet Col AB update and SQLite log **sequentially** per account (no watch channel posting here).
"""
from __future__ import annotations

import calendar
import logging
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from slack_sdk.errors import SlackApiError

from domain.slack.channel_utils import remember_slack_channel_id
from domain.integrations.gsheet_exporter import (
    CLASSIFICATION_COL_1BASE,
    _extract_opp_id_from_account_cell,
    _money_cell_to_float,
    _parse_burn_rate_from_sheet_cell,
    _parse_hyperlink_account_label,
    ensure_worksheet_min_columns,
)
from domain.salesforce.org62_client import lookup_record_channel
from domain.tracking.account_tracker import has_prior_outreach_log, log_outreach_event
from log_utils import log_debug

logger = logging.getLogger(__name__)

_sheet_protect_write_lock = threading.Lock()

OUTREACH_TRIGGER = "Outreach Initiated"
OUTREACH_DONE_MARK = "Outreach Initiated ✓"

_SLUG_MAX_MIDDLE = 40


@dataclass
class Stage3ProtectPhaseResult:
    """Protect-channel work (runs in parallel for batch scans); finalize updates sheet + SQLite."""

    ok: bool
    row: dict
    account_nm: str = ""
    sheet_row_index: int = 0
    pid: str = ""
    protect_ch_name: str = ""
    outreach_ts: str = ""
    atr_f: float = 0.0
    classification: str = "N/A"
    is_re_outreach: bool = False
    error: str | None = None


def _normalized_protect_channel_slug(name: str | None) -> str:
    return (name or "").strip().lstrip("#").lower()


def _is_slack_conversation_id(cell: str) -> bool:
    """
    Slack channel-style id (public ``C…`` — user spec — and private ``G…`` conversations).
    Stored verbatim in Col AC so ``name_taken`` rows avoid name lookup.
    """
    s = (cell or "").strip()
    # Typical Web API ids start with C (public); G(private) excluded from user wording but kept.
    return bool(re.match(r"^[CG][A-Za-z0-9]{8,}$", s))


def _read_protect_channel_cell(worksheet: Any | None, row_idx: int) -> str:
    """Col AC (column 29) — fresh read for ``name_taken`` handling vs stale row dict."""
    if worksheet is None or row_idx < 2:
        return ""
    try:
        cell = worksheet.cell(row_idx, 29)
        return str((cell.value or "")).strip()
    except Exception as e:
        logger.warning("Stage 3: Col AC read failed: %s", e)
        return ""


def _write_protect_channel_cell(worksheet: Any, row_idx: int, channel_id: str) -> None:
    """Col AC (column 29, 1-based) — store Slack conversation id (not name)."""
    if worksheet is None or row_idx < 2:
        return
    cid = (channel_id or "").strip()
    if not cid:
        return
    try:
        with _sheet_protect_write_lock:
            worksheet.update_cell(row_idx, 29, cid)
    except Exception as e:
        logger.warning("Stage 3: Col AC (Protect Channel) write failed: %s", e)


def _stage3_max_workers() -> int:
    try:
        return max(1, min(12, int(os.getenv("GM_REVIEW_STAGE3_MAX_WORKERS", "3"))))
    except ValueError:
        return 3


def _stage3_protect_phase(
    slack_client: Any,
    worksheet: Any,
    row: dict,
    trigger_user_id: str | None,
    *,
    is_re_outreach: bool,
) -> Stage3ProtectPhaseResult:
    """Join/create protect channel, invite team, post intervention — safe to run concurrently."""
    account_nm = (row.get("account_nm") or "").strip()
    sheet_row_index = int(row.get("sheet_row_index") or 0)

    status = (row.get("outreach_status") or "").strip()
    if status != OUTREACH_TRIGGER:
        return Stage3ProtectPhaseResult(
            ok=False,
            row=row,
            account_nm=account_nm or (row.get("account_name") or "Unknown"),
            sheet_row_index=sheet_row_index,
            error=f"unexpected status {status!r}",
        )

    if not account_nm or sheet_row_index < 2:
        logger.warning("Stage 3: missing account name or sheet row index")
        return Stage3ProtectPhaseResult(
            ok=False,
            row=row,
            account_nm=account_nm,
            sheet_row_index=sheet_row_index,
            error="missing account or row index",
        )

    atr_f = float(row.get("atr_value") or row.get("forecasted_attrition") or 0)
    classification = (row.get("classification") or "N/A").strip()
    now_iso = datetime.utcnow().isoformat() + "Z"

    slack_id_sheet = (row.get("protect_slack_channel_id") or "").strip()
    slug_sheet = _normalized_protect_channel_slug(row.get("protect_channel_slug"))

    pid, protect_ch_name = find_or_create_protect_channel(
        slack_client,
        account_nm,
        worksheet=worksheet,
        sheet_row_index=sheet_row_index,
        protect_slack_channel_id=slack_id_sheet if slack_id_sheet else None,
        protect_channel_slug=slug_sheet if slug_sheet else None,
        opp_id=str(row.get("opp_id") or "").strip(),
        trigger_user_id=trigger_user_id,
    )
    if not pid:
        logger.error("Stage 3: failed protect channel for %s", account_nm)
        return Stage3ProtectPhaseResult(
            ok=False,
            row=row,
            account_nm=account_nm,
            sheet_row_index=sheet_row_index,
            atr_f=atr_f,
            classification=classification,
            is_re_outreach=is_re_outreach,
            error="protect channel unavailable",
        )

    try:
        _invite_account_team_by_email(slack_client, pid, row)
    except Exception as e:
        logger.warning("Stage 3: AE/RM/CSM invite pass failed (non-fatal): %s", e)

    try:
        msg = build_outreach_message(row, is_re_outreach=is_re_outreach)
        slack_client.chat_postMessage(channel=pid, text=msg)
    except Exception as e:
        logger.exception("Stage 3: protect channel post failed: %s", e)
        return Stage3ProtectPhaseResult(
            ok=False,
            row=row,
            account_nm=account_nm,
            sheet_row_index=sheet_row_index,
            pid=pid,
            protect_ch_name=protect_ch_name,
            atr_f=atr_f,
            classification=classification,
            is_re_outreach=is_re_outreach,
            error=str(e)[:240],
        )

    return Stage3ProtectPhaseResult(
        ok=True,
        row=row,
        account_nm=account_nm,
        sheet_row_index=sheet_row_index,
        pid=pid,
        protect_ch_name=protect_ch_name,
        outreach_ts=now_iso,
        atr_f=atr_f,
        classification=classification,
        is_re_outreach=is_re_outreach,
    )


def _stage3_finalize_sequence(worksheet, pr: Stage3ProtectPhaseResult) -> bool:
    """Col AB ✓, ``outreach_log`` — sequential to avoid Sheet / ordering races."""
    if not pr.ok:
        return False

    try:
        _update_outreach_sheet_cell(worksheet, pr.sheet_row_index, OUTREACH_DONE_MARK)
    except Exception as e:
        logger.exception("Stage 3: sheet update failed: %s", e)
        return False

    try:
        log_outreach_event(
            account_nm=pr.account_nm,
            opp_id=str(pr.row.get("opp_id") or ""),
            protect_channel_id=pr.pid,
            protect_channel_name=pr.protect_ch_name,
            watch_channel_id="",
            outreach_ts=pr.outreach_ts,
            watch_ts="",
        )
    except Exception as e:
        logger.warning("Stage 3: outreach_log insert failed: %s", e)

    log_debug(f"Stage 3: outreach complete for {pr.account_nm}")
    return True

# Default Org62 Lightning host (override with SF_LIGHTNING_BASE if needed)
_DEFAULT_LIGHTNING = "https://org62.lightning.force.com"

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

# Slack ``conversations.join`` / ``conversations.invite`` errors we treat as success.
_JOIN_OK_ERRORS = frozenset({"already_in_channel"})
_INVITE_OK_ERRORS = frozenset({"already_in_channel", "cant_invite_self"})


def _extract_email_from_cell(text: str) -> str | None:
    """Best-effort email from a sheet cell (mailto:, raw address, or embedded in text)."""
    s = (text or "").strip()
    if not s:
        return None
    m = re.search(r"mailto:\s*([^\s|'\"<>\)]+)", s, re.I)
    if m:
        addr = m.group(1).strip().rstrip(">;").split("?")[0]
        mm = _EMAIL_RE.match(addr) or _EMAIL_RE.search(addr)
        if mm:
            return mm.group(0).strip().lower()
    m = _EMAIL_RE.search(s)
    return m.group(0).strip().lower() if m else None


def _ensure_bot_joined_protect_channel(slack_client: Any, channel_id: str) -> None:
    try:
        slack_client.conversations_join(channel=channel_id)
    except SlackApiError as e:
        code = ""
        try:
            code = (e.response or {}).get("error") or ""
        except Exception:
            pass
        if code in _JOIN_OK_ERRORS:
            return
        logger.warning("Stage 3: conversations_join bot failed for %s: %s", channel_id, code)


def _invite_one_slack_user_to_channel(
    slack_client: Any, channel_id: str, user_id: str
) -> None:
    if not channel_id or not user_id:
        return
    try:
        slack_client.conversations_invite(channel=channel_id, users=user_id.strip())
    except SlackApiError as e:
        code = ""
        try:
            code = (e.response or {}).get("error") or ""
        except Exception:
            pass
        if code in _INVITE_OK_ERRORS:
            return
        logger.warning(
            "Stage 3: conversations_invite user=%s channel=%s: %s",
            user_id,
            channel_id,
            code,
        )


def _warm_protect_channel_membership(
    slack_client: Any,
    channel_id: str | None,
    trigger_user_id: str | None,
) -> None:
    if not channel_id:
        return
    _ensure_bot_joined_protect_channel(slack_client, channel_id)
    tid = (trigger_user_id or "").strip()
    if tid:
        _invite_one_slack_user_to_channel(slack_client, channel_id, tid)


def _slack_user_ids_for_emails(slack_client: Any, emails: list[str]) -> list[str]:
    """Resolve workspace Slack user IDs via ``users_lookupByEmail`` (needs ``users:read.email``)."""
    ids: list[str] = []
    seen: set[str] = set()
    for email in emails:
        em = (email or "").strip().lower()
        if not em:
            continue
        try:
            r = slack_client.users_lookupByEmail(email=em)
            if r.get("ok") and r.get("user"):
                uid = str((r.get("user") or {}).get("id") or "").strip()
                if uid and uid not in seen:
                    seen.add(uid)
                    ids.append(uid)
        except SlackApiError as e:
            code = ""
            try:
                code = (e.response or {}).get("error") or ""
            except Exception:
                pass
            if code == "users_not_found":
                logger.debug("Stage 3: no Slack user for email %s", em)
            else:
                logger.warning(
                    "Stage 3: users_lookupByEmail failed for %s: %s", em, code
                )
    return ids


def _ae_rm_csm_emails_from_row(row: dict) -> list[str]:
    cells = [
        row.get("ae") or "",
        row.get("renewal_manager") or row.get("renewal_mgr") or "",
        row.get("csm") or "",
    ]
    out: list[str] = []
    seen: set[str] = set()
    for c in cells:
        em = _extract_email_from_cell(str(c))
        if em and em not in seen:
            seen.add(em)
            out.append(em)
    return out


def _invite_account_team_by_email(slack_client: Any, channel_id: str, row: dict) -> None:
    emails = _ae_rm_csm_emails_from_row(row)
    if not emails:
        return
    user_ids = _slack_user_ids_for_emails(slack_client, emails)
    for uid in user_ids:
        _invite_one_slack_user_to_channel(slack_client, channel_id, uid)


def build_protect_channel_name(account_name: str) -> str:
    """
    Slugify *account_name* (middle segment max 40 chars) and return
    ``{prefix}-{slug}-{suffix}`` from env (e.g. ``cc-zwilling-protect``).
    """
    slug = _slugify_account_segment(account_name)
    p = (os.getenv("CC_PROTECT_CHANNEL_PREFIX") or "cc").strip().lower()
    s = (os.getenv("CC_PROTECT_CHANNEL_SUFFIX") or "protect").strip().lower()
    name = f"{p}-{slug}-{s}"
    if len(name) > 80:
        name = name[:80].rstrip("-")
    return name


def _slugify_account_segment(name: str) -> str:
    raw = (name or "").lower()
    raw = re.sub(r"[^a-z0-9]+", "-", raw)
    raw = re.sub(r"-+", "-", raw).strip("-") or "account"
    if len(raw) > _SLUG_MAX_MIDDLE:
        raw = raw[:_SLUG_MAX_MIDDLE].rstrip("-")
    return raw or "account"


def _renewal_banner(renewal_text: str) -> tuple[str, str]:
    """(display_fragment, urgency_suffix) urgency_suffix is ' — URGENT' or ''."""
    s = (renewal_text or "").strip()
    if not s:
        return ("N/A", "")
    urgency = ""
    anchor = _parse_renewal_to_end_of_month_date(s)
    if anchor:
        today = date.today()
        days = (anchor - today).days
        if 0 <= days <= 90:
            urgency = " — URGENT"
    return (s, urgency)


def _parse_renewal_to_end_of_month_date(s: str) -> date | None:
    """Best-effort: YYYY-MM, YYYY-MM-DD, 'July 2026', etc."""
    s = (s or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    if len(s) >= 7 and s[4] == "-" and s[6:].replace("-", "").isdigit():
        try:
            y, m = int(s[:4]), int(s[5:7])
            last = calendar.monthrange(y, m)[1]
            return date(y, m, last)
        except ValueError:
            return None
    mon_map = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    m = re.match(
        r"^\s*([A-Za-z]+)\s+(\d{4})\s*$",
        s,
    )
    if m:
        mon_word, year_s = m.group(1).lower(), m.group(2)
        mi = mon_map.get(mon_word)
        if mi:
            try:
                y = int(year_s)
                last = calendar.monthrange(y, mi)[1]
                return date(y, mi, last)
            except ValueError:
                return None
    return None


def _fmt_money_short(n: float) -> str:
    v = abs(float(n or 0))
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if v >= 1000:
        return f"${v / 1000:.0f}K"
    return f"${v:,.0f}"


def _burn_severity_suffix(burn_display: str) -> str:
    t = (burn_display or "").upper()
    if "CRITICAL" in t:
        return " — :red_circle: CRITICAL"
    if "HIGH RISK" in t:
        return " — :large_orange_circle: HIGH RISK"
    br = _parse_burn_rate_from_sheet_cell(burn_display or "")
    if br is not None and br >= 1.0:
        return " — :red_circle: CRITICAL"
    return ""


def _is_red_flag_line(red_cell: str) -> bool:
    r = (red_cell or "").strip().lower()
    return bool(r) and (
        r.startswith("yes")
        or "red account" in r
        or r.startswith("http")
    )


def build_outreach_message(row: dict, *, is_re_outreach: bool = False) -> str:
    account = (row.get("account_nm") or row.get("account_name") or "Unknown").strip()
    atr_f = float(row.get("atr_value") or row.get("forecasted_attrition") or 0)
    atr_s = (row.get("atr_display") or "").strip() or _fmt_money_short(atr_f)

    fcast_s = (row.get("forecast_display") or "").strip()
    if not fcast_s and row.get("forecasted_attrition") is not None:
        fv = float(row.get("forecasted_attrition") or 0)
        fcast_s = f"-{_fmt_money_short(fv)}" if fv else "N/A"

    renew_s, urgent = _renewal_banner(row.get("renewal_month") or "")
    burn_line = (row.get("burn_rate_display") or "N/A").strip()
    burn_suffix = _burn_severity_suffix(burn_line)

    red_cell = (row.get("red_ac_flag") or "").strip()
    red_line = ""
    if _is_red_flag_line(red_cell):
        red_line = f"\n*:triangular_flag_on_post: Red Account:* {red_cell}"

    classification = (
        (row.get("classification") or row.get("_classification_cell") or "N/A")
        .strip()
    )
    ae = (row.get("ae") or "N/A").strip()
    rm = (row.get("renewal_manager") or row.get("renewal_mgr") or "N/A").strip()
    csm = (row.get("csm") or "N/A").strip()

    opp_id = (row.get("opp_id") or "").strip()
    opp_link = _opp_lightning_url(opp_id)

    if is_re_outreach:
        title = (
            f":arrows_counterclockwise: *Re-outreach — {account}* _(previously initiated)_"
        )
    else:
        title = f":rotating_light: *Commerce Attrition Intervention — {account}*"

    lines = [
        title,
        "",
        f"*💰 ATR:* {atr_s} | *Forecast:* {fcast_s}",
        f"*📅 Renewal:* {renew_s}{urgent}",
        f"*🔥 Burn Rate:* {burn_line}{burn_suffix}",
    ]
    if red_line:
        lines.append(red_line.strip())
    lines.extend(
        [
            f"*📋 Classification:* {classification}",
            "",
            f"*👤 AE:* {ae} | *RM:* {rm} | *CSM:* {csm}",
            "",
            "*Recommended Actions:*",
            "- Review contract restructure options",
            "- Schedule EBC with customer",
            "- Escalate to GM for executive outreach",
            "",
            (f"<{opp_link}|Org62 Opp>" if opp_link else "Org62 Opp: (missing opp id)"),
        ]
    )
    return "\n".join(lines)


def _opp_lightning_url(opp_id: str) -> str:
    oid = (opp_id or "").strip()
    if not oid:
        return ""
    base = (os.getenv("SF_LIGHTNING_BASE") or _DEFAULT_LIGHTNING).rstrip("/")
    return f"{base}/lightning/r/Opportunity/{oid}/view"


def find_or_create_protect_channel(
    slack_client: Any,
    account_name: str,
    *,
    worksheet: Any | None,
    sheet_row_index: int,
    protect_slack_channel_id: str | None = None,
    protect_channel_slug: str | None = None,
    opp_id: str | None = None,
    trigger_user_id: str | None = None,
) -> tuple[str | None, str]:
    """
    Resolve protect channel Id in order:

    1. Col **AC** id if already present (validated ``C…`` / ``G…``).
    2. Salesforce ``SlackChannelRelatedRecord`` for ``opp_id`` (when set).
    3. ``conversations.create`` for ``cc-<slug>-protect`` (env-driven prefix/suffix).
    4. ``name_taken``: re-read Col **AC**; use id + warm membership, or manual-backfill message.
    """

    cid_sheet = (protect_slack_channel_id or "").strip()
    slug_in = _normalized_protect_channel_slug(protect_channel_slug)
    ch_name = slug_in if slug_in else build_protect_channel_name(account_name)
    slug = ch_name
    opp = (opp_id or "").strip()

    def _complete(cid: str) -> tuple[str | None, str]:
        remember_slack_channel_id(slug, cid)
        _write_protect_channel_cell(worksheet, sheet_row_index, cid)
        _warm_protect_channel_membership(slack_client, cid, trigger_user_id)
        return cid, slug

    def _normalize_conversation_id(raw: str) -> str | None:
        s = (raw or "").strip()
        if not s:
            return None
        if _is_slack_conversation_id(s):
            return s
        up = s.upper()
        return up if _is_slack_conversation_id(up) else None

    if cid_sheet:
        norm = _normalize_conversation_id(cid_sheet)
        if not norm:
            logger.error(
                "Stage 3: Col AC must be a Slack conversation id (e.g. C… or G…), got %r",
                cid_sheet,
            )
            return None, slug
        return _complete(norm)

    if opp:
        try:
            sf_oid = (
                os.getenv("SALESFORCE_ORG_ID", "00D000000000062EAA") or ""
            ).strip()
            cid_sf = lookup_record_channel(org_id=[sf_oid], record_ids=[opp])
            if cid_sf:
                norm_sf = _normalize_conversation_id(cid_sf)
                if norm_sf:
                    return _complete(norm_sf)
        except Exception as e:
            logger.warning("Stage 3: lookup_record_channel failed for opp %s: %s", opp, e)

    try:
        resp = slack_client.conversations_create(name=ch_name, is_private=False)
        cid = str((resp.get("channel") or {}).get("id") or "").strip()
        if cid:
            return _complete(cid)
        logger.error("conversations_create missing channel id for #%s", ch_name)
        return None, slug
    except SlackApiError as e:
        try:
            err = (e.response.get("error") or "") if e.response else ""
        except Exception:
            err = ""
        if err != "name_taken":
            logger.error("conversations_create failed for #%s: %s", ch_name, err or e)
            return None, slug

        live_ac = _read_protect_channel_cell(worksheet, sheet_row_index)
        norm_live = _normalize_conversation_id(live_ac)
        if norm_live:
            remember_slack_channel_id(slug, norm_live)
            _warm_protect_channel_membership(
                slack_client, norm_live, trigger_user_id
            )
            return norm_live, slug

        if not (live_ac or "").strip():
            logger.error(
                "Channel exists but ID unknown — manually add channel ID to Col AC "
                "for %s",
                account_name,
            )
        else:
            logger.error(
                "Stage 3: name_taken for #%s — Col AC is not a valid Slack id (%r)",
                ch_name,
                live_ac[:80],
            )
        return None, slug
    except Exception as e:
        logger.exception("find_or_create_protect_channel: %s", e)
        return None, slug


def _update_outreach_sheet_cell(worksheet, sheet_row_index: int, value: str) -> None:
    worksheet.batch_update(
        [
            {
                "range": f"AB{sheet_row_index}",
                "values": [[value]],
            }
        ],
        value_input_option="USER_ENTERED",
    )


def sheet_row_to_stage3_dict(
    values: list[str],
    sheet_row_index: int,
    header_to_idx: dict[str, int],
) -> dict:
    """Build a Stage 3 row dict from raw sheet columns (header name → cell)."""

    def c(name: str, default_idx: int = 0) -> str:
        i = header_to_idx.get(name, default_idx) if header_to_idx else default_idx
        return (values[i] if i < len(values) else "").strip()

    account_cell = c("Account", 0)
    opp_id = _extract_opp_id_from_account_cell(account_cell)
    account_nm = _parse_hyperlink_account_label(account_cell) or account_cell

    cls_idx = header_to_idx.get("At-Risk Classification") if header_to_idx else None
    if cls_idx is None:
        cls_idx = CLASSIFICATION_COL_1BASE - 1
    classification = (
        values[cls_idx].strip()
        if cls_idx < len(values)
        else c("At-Risk Classification", 26)
    )

    def _money_from_col(name: str, idx: int) -> float:
        return _money_cell_to_float(c(name, idx))

    atr_money = _money_cell_to_float(c("ATR", 3))
    outreach_idx = header_to_idx.get("Outreach Status") if header_to_idx else None
    if outreach_idx is None:
        outreach_idx = 27

    protect_idx = header_to_idx.get("Protect Channel") if header_to_idx else None
    if protect_idx is None:
        protect_idx = 28

    prot_raw = (
        values[protect_idx].strip() if protect_idx < len(values) else ""
    )
    if _is_slack_conversation_id(prot_raw):
        prot_slack_id = prot_raw.strip()
        prot_slug = ""
    else:
        prot_slack_id = ""
        prot_slug = _normalized_protect_channel_slug(prot_raw)

    return {
        "sheet_row_index": sheet_row_index,
        "account_nm": account_nm,
        "account_name": account_nm,
        "opp_id": opp_id,
        "account_cell": account_cell,
        "atr_display": c("ATR", 3),
        "atr_value": atr_money,
        "forecast_display": c("Forecasted Attrition", 4),
        "forecasted_attrition": _money_from_col("Forecasted Attrition", 4),
        "renewal_month": c("Renewal Month", 9),
        "burn_rate_display": c("Burn Rate", 6) or c("Util Rate", 6),
        "red_ac_flag": c("Red AC Flag", 8),
        "classification": classification,
        "_classification_cell": classification,
        "ae": c("AE", 18),
        "renewal_manager": c("Renewal Manager", 19),
        "renewal_mgr": c("Renewal Manager", 19),
        "csm": c("CSM", 20),
        "outreach_status": (
            values[outreach_idx].strip()
            if outreach_idx < len(values)
            else ""
        ),
        "protect_slack_channel_id": prot_slack_id,
        "protect_channel_slug": prot_slug,
    }


def run_stage3_outreach(
    slack_client: Any,
    worksheet,
    row: dict,
    trigger_user_id: str | None = None,
) -> bool:
    """
    Run outreach for a single sheet-backed *row* (sequential API path).

    Uses prior ``outreach_log`` rows to set *Re-outreach* messaging when GM resets Col AB from ✓ back
    to *Outreach Initiated*.
    """
    is_re = has_prior_outreach_log(
        str(row.get("opp_id") or ""),
        str(row.get("account_nm") or row.get("account_name") or ""),
    )
    pr = _stage3_protect_phase(
        slack_client,
        worksheet,
        row,
        trigger_user_id,
        is_re_outreach=is_re,
    )
    return _stage3_finalize_sequence(worksheet, pr)


def scan_sheet_for_outreach(
    slack_client: Any, worksheet, trigger_user_id: str | None = None
) -> int:
    """
    Scan all populated rows for Col AB *Outreach Initiated* (not checkmark suffix)
    and run Stage 3.

    *trigger_user_id*: Slack user Id of whoever ran ``/initiate-outreach`` (invited to
    each protect channel). Omitted for automated export / bulk paths.
    """
    if os.getenv("GM_REVIEW_STAGE3_OUTREACH", "1").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        return 0

    ensure_worksheet_min_columns(worksheet, 31)

    try:
        all_vals = worksheet.get_all_values()
    except Exception as e:
        logger.warning("Stage 3: get_all_values failed: %s", e)
        return 0

    if not all_vals:
        return 0

    headers = [h.strip() for h in all_vals[0]]
    header_map = {h: i for i, h in enumerate(headers) if h}

    pending: list[tuple[dict, bool]] = []
    for offset, vals in enumerate(all_vals[1:], start=2):
        rd = sheet_row_to_stage3_dict(vals, offset, header_map)
        ost = (rd.get("outreach_status") or "").strip()
        if ost != OUTREACH_TRIGGER:
            continue
        is_re = has_prior_outreach_log(
            str(rd.get("opp_id") or ""),
            str(rd.get("account_nm") or rd.get("account_name") or ""),
        )
        pending.append((rd, is_re))

    if not pending:
        return 0

    workers = min(_stage3_max_workers(), len(pending))

    protect_results: list[Stage3ProtectPhaseResult | None] = [None] * len(pending)

    def _work(i: int, rd: dict, is_re: bool) -> tuple[int, Stage3ProtectPhaseResult]:
        try:
            return (
                i,
                _stage3_protect_phase(
                    slack_client,
                    worksheet,
                    rd,
                    trigger_user_id,
                    is_re_outreach=is_re,
                ),
            )
        except Exception as e:
            logger.exception("Stage 3 protect phase worker failed: %s", e)
            nm = (rd.get("account_nm") or rd.get("account_name") or "").strip()
            return (
                i,
                Stage3ProtectPhaseResult(
                    ok=False,
                    row=rd,
                    account_nm=nm,
                    error=str(e)[:240],
                ),
            )

    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = [
            ex.submit(_work, i, rd, ir) for i, (rd, ir) in enumerate(pending)
        ]
        for fut in futs:
            i, pr = fut.result()
            protect_results[i] = pr

    n = 0
    for pr in protect_results:
        if pr is None:
            continue
        if not pr.ok:
            logger.warning(
                "Stage 3 protect phase skipped finalize for %s: %s",
                pr.account_nm,
                pr.error,
            )
            continue
        if _stage3_finalize_sequence(worksheet, pr):
            n += 1
    return n
