"""
Protect-channel deltas + 24h activity helpers for Commerce Attrition digest.
Rollup messaging lives in ``services.attrition_digest`` (single combined post).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from slack_sdk.errors import SlackApiError

from domain.integrations.gsheet_exporter import (
    _parse_burn_rate_from_sheet_cell,
)
from domain.tracking.account_tracker import (
    account_snapshot_key,
    get_account_snapshot,
    upsert_account_snapshot,
)
from services.attrition_outreach import _fmt_money_short

logger = logging.getLogger(__name__)

_SWING_MIN_DELTA_USD = 50_000.0
_BURN_MIN_DELTA = 0.1

_ACTIVITY_SKIP_SUBTYPES = frozenset(
    {
        "channel_join",
        "channel_leave",
        "channel_topic",
        "channel_purpose",
        "channel_name",
        "channel_archive",
        "channel_unarchive",
        "pinned_item",
        "reminder_add",
    }
)


def _fcst_delta_significant(old: float, new: float) -> bool:
    ao, an = float(old or 0), float(new or 0)
    if ao == 0 and an == 0:
        return False
    if abs(ao) < 1.0:
        return abs(an) >= 100_000.0
    return abs(an - ao) / max(abs(ao), 1.0) > 0.10


def detect_changes(old_snapshot: dict | None, current_row: dict) -> list[str]:
    """
    Human-readable deltas when thresholds are met vs ``account_snapshot``.
    ``current_row``: ``forecasted_atr``, ``swing``, ``classification``, ``burn_parse``,
    ``burn_disp``, ``renewal_month``, ``red_ac_flag``.
    """
    if not old_snapshot:
        return []

    fcst_new = float(current_row.get("forecasted_atr") or 0)
    swing_new = float(current_row.get("swing") or 0)
    cls_new = str(current_row.get("classification") or "").strip()
    burn_p_new = current_row.get("burn_parse")
    burn_disp_new = str(current_row.get("burn_disp") or "").strip()
    renew_new = str(current_row.get("renewal_month") or "").strip()
    red_new = str(current_row.get("red_ac_flag") or "").strip()

    fcst_old = float(old_snapshot.get("forecasted_atr") or 0)
    swing_old = float(old_snapshot.get("swing") or 0)
    cls_old = str(old_snapshot.get("classification") or "").strip()

    bor = old_snapshot.get("burn_rate")
    try:
        burn_old_p = float(bor) if bor is not None else None
    except (TypeError, ValueError):
        burn_old_p = None
    burn_disp_old = ""
    if burn_old_p is not None:
        burn_disp_old = f"{burn_old_p:.2f}"

    renew_old = str(old_snapshot.get("renewal_month") or "").strip()
    red_old = str(old_snapshot.get("red_ac_flag") or "").strip()

    lines: list[str] = []

    if _fcst_delta_significant(fcst_old, fcst_new):
        worse = abs(fcst_new) > abs(fcst_old)
        arrow = "⬆️" if worse else "⬇️"
        lines.append(
            f"- ATR: {_fmt_money_short(fcst_old)} → {_fmt_money_short(fcst_new)} {arrow}"
        )

    if abs(swing_new - swing_old) > _SWING_MIN_DELTA_USD:
        improved = swing_new > swing_old
        icon = "✅" if improved else "⬇️"
        lines.append(
            f"- Swing: {_fmt_money_short(swing_old)} → {_fmt_money_short(swing_new)} {icon}"
        )

    if cls_old != cls_new:
        lines.append(f"- Classification: {cls_old or '—'} → {cls_new or '—'} 🔄")

    if red_old != red_new:
        lines.append(f"- Red Account status: {red_old or '—'} → {red_new or '—'} 🚩")

    if renew_old != renew_new:
        lines.append(
            f"- Renewal Month: {renew_old or '—'} → {renew_new or '—'} 📅"
        )

    if _burn_delta_significant(burn_old_p, burn_p_new, burn_disp_old, burn_disp_new):
        lines.append(f"- Burn rate: {burn_disp_old or '—'} → {burn_disp_new or '—'} 🔥")

    return lines


def _burn_delta_significant(
    po: Optional[float],
    pn: Optional[float],
    do: str,
    dn: str,
) -> bool:
    if po is not None and pn is not None:
        return abs(float(pn) - float(po)) >= _BURN_MIN_DELTA
    return (do or "").strip() != (dn or "").strip()


FetchActivityRaw = tuple[
    int, str, float, list[str]
]  # n_msgs, preview, latest_ts, human_user_ids (unique order)


def _fetch_activity_raw(
    slack_client: Any, channel_id: str, *, hours: int = 24
) -> FetchActivityRaw | None:
    cid = (channel_id or "").strip()
    if not cid:
        return None

    now = datetime.now(timezone.utc)
    oldest_ts = (now - timedelta(hours=hours)).timestamp()
    raw: list[dict] = []
    cursor: str | None = None

    try:
        for _ in range(12):
            kwargs: dict[str, Any] = {
                "channel": cid,
                "oldest": str(oldest_ts),
                "limit": 200,
            }
            if cursor:
                kwargs["cursor"] = cursor
            resp = slack_client.conversations_history(**kwargs)
            if not resp.get("ok"):
                return None
            raw.extend(resp.get("messages") or [])
            cursor = (resp.get("response_metadata") or {}).get("next_cursor")
            if not cursor:
                break
    except SlackApiError as e:
        logger.debug("conversations_history %s: %s", cid, e)
        return None
    except Exception as e:
        logger.warning("conversations_history %s failed: %s", cid, e)
        return None

    qualifying: list[dict] = []
    for m in raw:
        st = (m.get("subtype") or "").strip()
        if st and st in _ACTIVITY_SKIP_SUBTYPES:
            continue
        if st in ("message_changed", "message_deleted", "tombstone"):
            continue
        txt = (m.get("text") or "").strip()
        if not txt:
            continue
        qualifying.append(m)

    if not qualifying:
        return None

    qualifying.sort(key=lambda m: float(m.get("ts") or 0), reverse=True)
    latest_m = qualifying[0]
    latest_ts = float(latest_m.get("ts") or 0)

    human_order: list[str] = []
    seen: set[str] = set()
    for m in qualifying:
        uid = str(m.get("user") or "").strip()
        if uid and uid not in seen:
            seen.add(uid)
            human_order.append(uid)
    latest_text = latest_m.get("text") or ""

    top = latest_text.strip()
    if len(top) > 100:
        top = top[:99].rstrip() + "…"

    return len(qualifying), top, latest_ts, human_order


def _resolve_users_display_ring(
    slack_client: Any, uids: list[str], cache: dict[str, str], *, ring: dict[str, str]
) -> str:
    """Short display string like ``Taylor Remsen`` or ``A & B & 2 others``."""
    if not uids:
        return "participants"

    names: list[str] = []
    err_once = False
    for uid in uids[:8]:
        nm = ring.get(uid) or cache.get(uid)
        if nm is None:
            try:
                info = slack_client.users_info(user=uid)
                u = (info or {}).get("user") or {}
                nm = (
                    str(u.get("real_name") or "")
                    .strip()
                    or str((u.get("profile") or {}).get("real_name") or "").strip()
                    or str((u.get("profile") or {}).get("display_name") or "").strip()
                    or str(u.get("name") or "").strip()
                    or uid
                )
                cache[uid] = nm
            except Exception:
                if not err_once:
                    logger.debug("users_info(%s) failed", uid)
                    err_once = True
                nm = uid[:8] + "…" if len(uid) > 8 else uid
        names.append(nm)
        ring[uid] = nm

    n = len(uids)
    if n == 1:
        return names[0]
    if n == 2:
        return f"{names[0]} & {names[1]}"
    if n <= 4:
        return " & ".join(names[: min(n, 4)])

    tail = ", ".join(names[:3])
    return f"{tail} & {n - 3} others"


def digest_activity_who_line(
    slack_client: Any,
    *,
    raw: FetchActivityRaw | None,
    uid_cache: dict[str, str],
    ring_names: dict[str, str],
) -> tuple[str, str | None]:
    """
    Returns ``(💬 line, Latest line or None)`` for digest body.
    """
    if raw is None:
        return "💬 No new messages", None
    n_msg, preview, _ts, huids = raw
    who = _resolve_users_display_ring(slack_client, huids, uid_cache, ring=ring_names)
    act = (
        f"💬 {n_msg} messages from {who}"
        if who != "participants"
        else f"💬 {n_msg} messages from participants"
    )
    lat = f'Latest: "{preview}"'
    return act, lat


def digest_change_lines(change_bullets: list[str]) -> list[str]:
    """Turn ``detect_changes`` bullets into ``📉 …`` digest lines."""
    out: list[str] = []
    for ln in change_bullets:
        s = ln.strip()
        if s.startswith("- "):
            s = s[2:].strip()
        out.append(f"📉 {s}")
    return out


def digest_metrics_from_sheet_row(rd: dict) -> dict:
    """Build ``detect_changes`` / snapshot row shape from GM Review dict."""
    burn_disp = (rd.get("burn_rate_display") or "").strip()
    burn_p = _parse_burn_rate_from_sheet_cell(burn_disp)
    return {
        "forecasted_atr": float(rd.get("forecasted_attrition") or 0),
        "swing": float(rd.get("swing_value") or 0),
        "classification": (rd.get("classification") or "").strip(),
        "burn_parse": burn_p,
        "burn_disp": burn_disp,
        "renewal_month": (rd.get("renewal_month") or "").strip(),
        "red_ac_flag": (rd.get("red_ac_flag") or "").strip(),
    }


def process_digest_account_row(
    slack_client: Any,
    *,
    rd: dict,
    account_nm: str,
    opp_id_sheet: str,
    protect_channel_id: str,
    snap_date: str,
    uid_cache: dict[str, str],
    participant_ring: dict[str, str],
) -> str | None:
    """
    Produce one account section for digest (or ``None`` if no sheet delta and no Slack activity).
    Always upserts ``account_snapshot``.
    """
    key = account_snapshot_key(opp_id_sheet, account_nm)

    curr = digest_metrics_from_sheet_row(rd)
    prev = get_account_snapshot(key)
    deltas = detect_changes(prev, curr)
    raw_act = _fetch_activity_raw(slack_client, protect_channel_id)
    last_ts: float | None = None
    if raw_act is not None:
        last_ts = float(raw_act[2])

    def _save_snapshot() -> None:
        try:
            upsert_account_snapshot(
                account_key=key,
                account_nm=account_nm,
                opp_id=opp_id_sheet,
                protect_channel_id=protect_channel_id,
                forecasted_atr=float(curr.get("forecasted_atr") or 0),
                swing=float(curr.get("swing") or 0),
                classification=str(curr.get("classification") or ""),
                burn_rate=curr.get("burn_parse"),
                renewal_month=str(curr.get("renewal_month") or ""),
                red_ac_flag=str(curr.get("red_ac_flag") or ""),
                last_message_ts=last_ts,
                snapshot_date=snap_date,
            )
        except Exception as e:
            logger.warning("digest account upsert %s failed: %s", key, e)

    if not deltas and raw_act is None:
        _save_snapshot()
        return None

    who_line, latest_line = digest_activity_who_line(
        slack_client,
        raw=raw_act,
        uid_cache=uid_cache,
        ring_names=participant_ring,
    )
    lines_blk: list[str] = [
        f"📊 {account_nm}",
        who_line,
    ]
    if latest_line:
        lines_blk.append(latest_line)
    if deltas:
        lines_blk.extend(digest_change_lines(deltas))
    section = "\n".join(lines_blk).strip()

    _save_snapshot()
    return section
