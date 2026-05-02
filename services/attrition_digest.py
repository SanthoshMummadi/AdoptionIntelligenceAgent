"""
Commerce Attrition daily digest — rollup from ``outreach_log`` + GM Review Sheet.
Posts to ``#cc-attrition-watch`` (scheduled and manual slash command).

Named by behavior (`attrition_digest`), not pipeline stage numbering.

Per-account protect-channel activity + sheet deltas are merged into this digest via
``services.attrition_tracker.process_digest_account_row``.
"""
from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

import gspread
from slack_sdk.errors import SlackApiError

from domain.integrations.gsheet_exporter import (
    get_google_creds,
    ensure_worksheet_min_columns,
)
from domain.tracking.account_tracker import (
    get_latest_outreach_accounts_for_digest,
    log_digest_entry,
)
from services.attrition_outreach import (
    _is_slack_conversation_id,
    _parse_renewal_to_end_of_month_date,
    _fmt_money_short,
    sheet_row_to_stage3_dict,
)
from services.attrition_tracker import process_digest_account_row

logger = logging.getLogger(__name__)

IST = ZoneInfo("Asia/Kolkata")

_OUTREACH_PENDING = "Pending Review"

ACTIVE_OUTREACH = frozenset(
    {"Outreach Initiated", "Outreach Initiated ✓"},
)


def _norm_key_name(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _days_to_renewal(renewal_cell: str) -> Optional[int]:
    anchor = _parse_renewal_to_end_of_month_date(renewal_cell or "")
    if not anchor:
        return None
    return (anchor - date.today()).days


def _is_urgent_renewal(renewal_cell: str) -> bool:
    d = _days_to_renewal(renewal_cell)
    if d is None:
        return False
    return 0 <= d <= 90


@dataclass
class DigestSnapshot:
    cloud: str
    urgent_count: int
    outreach_count: int
    pending_count: int
    reviewed_count: int
    top_urgent: list[dict]
    matched_rows: int
    outreach_cohort: int
    active_outreach_rows: list[dict]


def _load_commerce_sheet_rows(worksheet_title: str) -> tuple[list[str], list[list[str]]]:
    from domain.integrations.gsheet_exporter import _gsheet_id

    sid = _gsheet_id()
    if not sid:
        raise RuntimeError("GSHEET_ID / GOOGLE_SHEET_ID not set")

    creds = get_google_creds()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sid)
    ws = sh.worksheet(worksheet_title)
    ensure_worksheet_min_columns(ws, 31)
    all_vals = ws.get_all_values()
    if not all_vals:
        return [], []
    headers = [h.strip() for h in all_vals[0]]
    return headers, all_vals[1:]


def _gather_snapshot(cloud: str, worksheet_title: str) -> DigestSnapshot:
    outreach_rows = get_latest_outreach_accounts_for_digest()
    by_opp: dict[str, dict] = {}
    by_name: dict[str, dict] = {}
    for r in outreach_rows:
        oid = str(r.get("opp_id") or "").strip()
        anm = str(r.get("account_nm") or "").strip()
        pid = str(r.get("protect_channel_id") or "").strip()
        if oid:
            by_opp[oid] = {"opp_id": oid, "account_nm": anm, "protect_channel_id": pid}
        elif anm:
            by_name[_norm_key_name(anm)] = {
                "opp_id": "",
                "account_nm": anm,
                "protect_channel_id": pid,
            }

    try:
        headers, data_rows = _load_commerce_sheet_rows(worksheet_title)
    except Exception as e:
        logger.exception("digest: sheet load failed: %s", e)
        raise

    if not headers:
        return DigestSnapshot(
            cloud=cloud,
            urgent_count=0,
            outreach_count=0,
            pending_count=0,
            reviewed_count=0,
            top_urgent=[],
            matched_rows=0,
            outreach_cohort=len(outreach_rows),
            active_outreach_rows=[],
        )

    header_map = {h: i for i, h in enumerate(headers) if h}

    matched: list[dict] = []
    for offset, vals in enumerate(data_rows, start=2):
        rd = sheet_row_to_stage3_dict(vals, offset, header_map)
        oid = str(rd.get("opp_id") or "").strip()
        label = str(
            rd.get("account_nm") or rd.get("account_name") or ""
        ).strip()
        rec: dict | None = None
        if oid and oid in by_opp:
            rec = by_opp[oid]
        else:
            nk = _norm_key_name(label)
            if nk and nk in by_name:
                rec = by_name[nk]
        if not rec:
            continue
        rd["_digest_channel_id"] = (
            rec.get("protect_channel_id") or rd.get("protect_slack_channel_id") or ""
        )
        matched.append(rd)

    urgent_count = 0
    outreach_count = 0
    pending_count = 0
    reviewed_count = 0
    urgent_pool: list[dict] = []

    for rd in matched:
        renew = str(rd.get("renewal_month") or "")
        cls = (rd.get("classification") or "").strip()
        ost = (rd.get("outreach_status") or "").strip()

        if _is_urgent_renewal(renew):
            urgent_count += 1
            urgent_pool.append(rd)
        if ost in ACTIVE_OUTREACH:
            outreach_count += 1
        if cls == _OUTREACH_PENDING:
            pending_count += 1
        if ost == "Reviewed":
            reviewed_count += 1

    urgent_pool.sort(
        key=lambda r: float(r.get("atr_value") or 0.0),
        reverse=True,
    )
    top_urgent = urgent_pool[:3]

    active_outreach_rows: list[dict] = []
    for rd in matched:
        ost = (rd.get("outreach_status") or "").strip()
        if ost not in ACTIVE_OUTREACH:
            continue
        ch = str(
            rd.get("_digest_channel_id")
            or rd.get("protect_slack_channel_id")
            or ""
        ).strip()
        if not _is_slack_conversation_id(ch):
            up = ch.upper()
            if _is_slack_conversation_id(up):
                ch = up
        if not _is_slack_conversation_id(ch):
            continue
        row = dict(rd)
        row["_norm_protect_channel_id"] = ch
        active_outreach_rows.append(row)

    return DigestSnapshot(
        cloud=cloud,
        urgent_count=urgent_count,
        outreach_count=outreach_count,
        pending_count=pending_count,
        reviewed_count=reviewed_count,
        top_urgent=top_urgent,
        matched_rows=len(matched),
        outreach_cohort=len(outreach_rows),
        active_outreach_rows=active_outreach_rows,
    )


_DIGEST_TEXT_MAX = 38000


def _format_digest_summary(snap: DigestSnapshot) -> str:
    today = datetime.now(IST).strftime("%B %d, %Y")
    lines = [
        f"📊 *Commerce Attrition — Daily Digest | {today}*",
        "",
        f"🔴 *URGENT (≤90 days):* {snap.urgent_count} accounts",
        f"⚡ *Active Outreach:* {snap.outreach_count} accounts",
        f"👀 *Pending Review:* {snap.pending_count} accounts",
        f"✅ *Reviewed:* {snap.reviewed_count} accounts",
        "",
    ]
    if snap.top_urgent:
        lines.append("*Top URGENT accounts:*")
        for rd in snap.top_urgent:
            nm = (
                str(rd.get("account_nm") or rd.get("account_name") or "Unknown")
            ).strip()
            cid = str(rd.get("_digest_channel_id") or "").strip()
            atr_f = float(rd.get("atr_value") or 0.0)
            atr_s = (
                str(rd.get("atr_display") or "").strip()
                or _fmt_money_short(atr_f)
            )
            renew = str(rd.get("renewal_month") or "").strip() or "N/A"
            if cid and cid[0] in ("C", "G"):
                lines.append(
                    f"- <#{cid}> — *{nm}* — {atr_s} ATR — {renew}"
                )
            else:
                lines.append(f"- *{nm}* — {atr_s} ATR — {renew}")
        lines.append("")
    if snap.outreach_cohort and snap.matched_rows < snap.outreach_cohort:
        lines.append(
            f"_Note: {snap.outreach_cohort} outreach log account(s); "
            f"{snap.matched_rows} matched current sheet rows._\n"
        )
    lines.append("_Run `/gm-review-sheet Commerce Cloud` to refresh_")
    return "\n".join(lines)


def _compose_full_digest_message(
    slack_client: Any, snap: DigestSnapshot, worksheet_title: str
) -> str:
    summary = _format_digest_summary(snap)
    if os.getenv("GM_REVIEW_ACCOUNT_UPDATES", "1").strip().lower() in (
        "0",
        "false",
        "no",
        "off",
    ):
        return summary

    if not slack_client:
        return summary

    snap_date = datetime.now(IST).strftime("%Y-%m-%d")
    uid_cache: dict[str, str] = {}
    participant_ring: dict[str, str] = {}
    sections: list[str] = []

    for rd in snap.active_outreach_rows:
        pch = str(rd.get("_norm_protect_channel_id") or "").strip()
        oid = str(rd.get("opp_id") or "").strip()
        label = (
            str(rd.get("account_nm") or rd.get("account_name") or "Unknown").strip()
        )
        blk = process_digest_account_row(
            slack_client,
            rd=rd,
            account_nm=label,
            opp_id_sheet=oid,
            protect_channel_id=pch,
            snap_date=snap_date,
            uid_cache=uid_cache,
            participant_ring=participant_ring,
        )
        if blk:
            sections.append(blk)

    if not sections:
        return summary

    body = summary.rstrip()
    assembled = body + "\n\n---\n\n" + "\n\n---\n\n".join(sections)
    if len(assembled) > _DIGEST_TEXT_MAX:
        logger.warning(
            "digest combined text truncated from %s chars",
            len(assembled),
        )
        assembled = assembled[: _DIGEST_TEXT_MAX - 20] + "\n_(truncated)_"
    return assembled


def build_daily_digest(
    slack_client: Any = None,
    *,
    cloud: str = "Commerce Cloud",
    worksheet_title: str | None = None,
) -> str:
    """Build digest text from ``outreach_log`` + the Commerce GM Review tab."""
    title = worksheet_title or os.getenv(
        "GM_REVIEW_GOOGLE_TAB", "Commerce Cloud GM Review"
    )
    snap = _gather_snapshot(cloud, title)
    return _compose_full_digest_message(slack_client, snap, title)


def run_daily_digest(
    slack_client: Any,
    *,
    cloud: str = "Commerce Cloud",
    worksheet_title: str | None = None,
) -> bool:
    """Post digest to ``CC_ATTRITION_WATCH_CHANNEL_ID`` and append ``digest_log``."""
    title = worksheet_title or os.getenv(
        "GM_REVIEW_GOOGLE_TAB", "Commerce Cloud GM Review"
    )
    watch_id = (os.getenv("CC_ATTRITION_WATCH_CHANNEL_ID") or "").strip()
    if not watch_id:
        logger.error("run_daily_digest: CC_ATTRITION_WATCH_CHANNEL_ID is not set")
        return False

    try:
        snap = _gather_snapshot(cloud, title)
        text = _compose_full_digest_message(slack_client, snap, title)
    except Exception as e:
        logger.exception("run_daily_digest: build failed: %s", e)
        return False

    posted_ts = datetime.now(IST).isoformat()
    try:
        slack_client.chat_postMessage(channel=watch_id, text=text)
    except SlackApiError as e:
        err = ""
        try:
            err = str((e.response or {}).get("error") or "")
        except Exception:
            err = ""
        if err == "is_archived":
            logger.error(
                "run_daily_digest: watch channel %r is archived — Slack rejects "
                "chat.postMessage. Unarchive the channel in Slack or set "
                "CC_ATTRITION_WATCH_CHANNEL_ID to an active channel.",
                watch_id,
            )
        elif err == "not_in_channel":
            logger.error(
                "run_daily_digest: bot is not in watch channel %r — invite the app "
                "or use a channel id the bot can post to.",
                watch_id,
            )
        elif err == "channel_not_found":
            logger.error(
                "run_daily_digest: channel %r not found — check CC_ATTRITION_WATCH_CHANNEL_ID.",
                watch_id,
            )
        else:
            logger.exception("run_daily_digest: chat_postMessage failed: %s", e)
        return False
    except Exception as e:
        logger.exception("run_daily_digest: chat_postMessage failed: %s", e)
        return False

    try:
        log_digest_entry(
            cloud=snap.cloud,
            urgent_count=snap.urgent_count,
            outreach_count=snap.outreach_count,
            reviewed_count=snap.reviewed_count,
            pending_count=snap.pending_count,
            watch_channel_id=watch_id,
            posted_ts=posted_ts,
        )
    except Exception as e:
        logger.warning("run_daily_digest: digest_log insert failed: %s", e)

    logger.info("Daily digest posted to %s", watch_id)
    return True
