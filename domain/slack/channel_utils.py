"""
Slack channel helpers — **no workspace-wide ``conversations.list`` directory build**.

Protect channels default to ``conversations.create`` first (``O(1)``).

``resolve_slack_channel_id`` (e.g. watch channel lookup) scans ``conversations.list`` pages until
there is an **exact name match**, then stops (early exit). Results are optionally memoized in-process.

See env: ``SLACK_CHANNEL_LOOKUP_MAX_PAGES``, ``SLACK_LOOKUP_LIST_*`` (rate-limit tuning).
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from typing import Any

from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)

# Lightweight per-process memo (exact Slack channel name lowercase → id)
_channel_name_memo: dict[str, str] = {}
_memo_lock = threading.Lock()

_SLACK_ID_RE = re.compile(r'\b[CGA][A-Z0-9]{8,}\b')


def _lookup_list_limit() -> int:
    try:
        return max(1, min(200, int(os.getenv("SLACK_CONVERSATIONS_LIST_LIMIT", "200"))))
    except ValueError:
        return 200


def _lookup_max_pages() -> int:
    try:
        return max(1, int(os.getenv("SLACK_CHANNEL_LOOKUP_MAX_PAGES", "25")))
    except ValueError:
        return 25


def _lookup_page_sleep_s() -> float:
    try:
        return max(0.0, float(os.getenv("SLACK_CHANNEL_LOOKUP_PAGE_SLEEP_S", "0.5")))
    except ValueError:
        return 0.5


def _conversation_list_rate_limit_retries() -> int:
    try:
        return max(3, int(os.getenv("SLACK_CONVERSATIONS_LIST_RATELIMIT_RETRIES", "20")))
    except ValueError:
        return 20


def _ratelimited_backoff_seconds(e: SlackApiError) -> float:
    cap = float(os.getenv("SLACK_CONVERSATIONS_LIST_RETRY_CAP_S", "120"))
    floor = float(os.getenv("SLACK_CONVERSATIONS_LIST_RETRY_FLOOR_S", "2"))
    raw = None
    try:
        if e.response is not None:
            raw = e.response.get("retry_after")
    except Exception:
        pass
    try:
        s = float(raw) if raw is not None else 30.0
    except (TypeError, ValueError):
        s = 30.0
    return min(max(s + 1.0, floor), cap)


def _conversations_list_one_page(slack_client: Any, cursor: str | None) -> Any | None:
    """One ``conversations.list`` page with ``ratelimited`` retries."""
    max_try = _conversation_list_rate_limit_retries()
    last_err = ""
    lim = _lookup_list_limit()
    for attempt in range(1, max_try + 1):
        try:
            return slack_client.conversations_list(
                cursor=cursor,
                limit=lim,
                types="public_channel,private_channel",
                exclude_archived=True,
            )
        except SlackApiError as e:
            try:
                last_err = (e.response.get("error") or "") if e.response else ""
            except Exception:
                last_err = ""
            if last_err == "ratelimited":
                wait = _ratelimited_backoff_seconds(e)
                logger.warning(
                    "Slack ratelimited on conversations.list; sleeping %.1fs (attempt %s/%s)",
                    wait,
                    attempt,
                    max_try,
                )
                time.sleep(wait)
                continue
            logger.warning(
                "conversations_list SlackApiError: %s",
                last_err or str(e),
            )
            return None
        except Exception as ex:
            logger.warning("conversations_list failed: %s", ex)
            return None
    logger.warning(
        "conversations.list still ratelimited after %s tries (last=%r)",
        max_try,
        last_err,
    )
    return None


def slack_channel_id_from_slack_api_error(exc: SlackApiError) -> str | None:
    """
    Best-effort channel id bundled with Slack error payloads (rare on ``name_taken``).

    Avoids iterating ``SlackResponse`` (pagination side effects).
    """
    try:
        r = getattr(exc, "response", None)
        if r is not None:
            ch = r.get("channel")
            if isinstance(ch, dict):
                cid = str(ch.get("id") or "").strip()
                if cid:
                    return cid
            cid = str(r.get("channel_id") or "").strip()
            if cid:
                return cid
    except Exception:
        pass

    try:
        m = _SLACK_ID_RE.search(str(exc))
        if m:
            return m.group(0)
    except Exception:
        pass
    return None


def slack_try_conversations_info_channel_id(
    slack_client: Any, channel_slug: str
) -> str | None:
    """
    Resolve a channel id via ``conversations.info`` using the normalized slug (sometimes also
    accepts ``#name``). No ``conversations.list`` scan.

    Official API documents *channel* as an id, but some workspaces accept a name-like ref; we
    probe both forms and return the first success.
    """
    slug = (channel_slug or "").strip().lstrip("#").lower()
    if not slug:
        return None

    for cand in (slug, f"#{slug}"):
        try:
            resp = slack_client.conversations_info(channel=cand)
            if not resp.get("ok"):
                continue
            ch = resp.get("channel") or {}
            cid = str(ch.get("id") or "").strip()
            if cid:
                return cid
        except SlackApiError:
            continue
        except Exception:
            continue
    return None


def lookup_slack_channel_id_by_exact_name(
    slack_client: Any,
    channel_name: str,
) -> str | None:
    """
    Paginate ``conversations.list`` until *channel_name* matches (case-insensitive), then stop.
    Bounded by ``SLACK_CHANNEL_LOOKUP_MAX_PAGES``.
    """
    target = (channel_name or "").strip().lstrip("#").lower()
    if not target:
        return None

    cursor: str | None = None
    max_pages = _lookup_max_pages()
    nap = _lookup_page_sleep_s()

    for _page_idx in range(max_pages):
        resp = _conversations_list_one_page(slack_client, cursor)
        if resp is None:
            return None

        for ch in resp.get("channels", []) or []:
            nm = (ch.get("name") or "").strip().lower()
            if nm == target:
                cid = str(ch.get("id") or "").strip()
                return cid or None

        cursor = (resp.get("response_metadata") or {}).get("next_cursor") or None
        if not cursor:
            break
        if nap:
            time.sleep(nap)

    logger.warning(
        "Channel %r not found after up to %s conversations.list pages",
        target,
        max_pages,
    )
    return None


def resolve_slack_channel_id(slack_client: Any, channel_name: str) -> str | None:
    """
    Return Slack channel id for ``channel_name`` (with or without leading ``#``).

    Uses bounded paginated listing with memoization — never loads the whole workspace up front.

    Requires ``channels:read`` (+ ``groups:read`` where applicable).
    """
    key = (channel_name or "").strip().lstrip("#").lower()
    if not key:
        return None

    with _memo_lock:
        cid = _channel_name_memo.get(key)
    if cid:
        return cid

    found = lookup_slack_channel_id_by_exact_name(slack_client, key)
    if found:
        with _memo_lock:
            _channel_name_memo[key] = found
    return found


def commerce_cc_protect_channel_name(middle_segment: str = "zwilling") -> str:
    """
    Build protect-channel slug from ``CC_PROTECT_CHANNEL_PREFIX`` / ``CC_PROTECT_CHANNEL_SUFFIX``
    and *middle_segment* (default ``zwilling`` → e.g. ``cc-zwilling-protect``).
    """
    p = (os.getenv("CC_PROTECT_CHANNEL_PREFIX") or "cc").strip()
    s = (os.getenv("CC_PROTECT_CHANNEL_SUFFIX") or "protect").strip()
    m = (middle_segment or "zwilling").strip()
    return f"{p}-{m}-{s}"


def remember_slack_channel_id(channel_name: str, channel_id: str) -> None:
    """Memoize ``name → id`` after ``conversations.create`` (skip list lookups)."""
    key = (channel_name or "").strip().lstrip("#").lower()
    cid = str(channel_id or "").strip()
    if key and cid:
        with _memo_lock:
            _channel_name_memo[key] = cid


def invalidate_slack_channel_cache_entry(channel_name: str) -> None:
    """Drop one memo entry (e.g. before re-resolving after ``name_taken``)."""
    key = (channel_name or "").strip().lstrip("#").lower()
    if key:
        with _memo_lock:
            _channel_name_memo.pop(key, None)


def clear_slack_channel_cache() -> None:
    """Tests / hot-reload: clear memoized name → id map."""
    with _memo_lock:
        _channel_name_memo.clear()
