import logging
from datetime import datetime

from domain.tracking.account_tracker import (
    get_active_tracked,
    get_tracking_summary,
    purge_closed,
)

logger = logging.getLogger(__name__)


def run_daily_pulse(slack_client, target_channel: str = None) -> str:
    """
    Daily pulse: summary of active tracked opps + alerts.
    """
    logger.info("Starting daily pulse...")

    purged = purge_closed()
    if purged > 0:
        logger.info(f"Purged {purged} closed/expired opps")

    active_opps = get_active_tracked()
    if not active_opps:
        msg = ":white_check_mark: *Daily Pulse* — No active strategic opps being tracked."
        if target_channel:
            slack_client.chat_postMessage(channel=target_channel, text=msg)
        logger.info("No active opps — pulse complete")
        return msg

    summary = get_tracking_summary()
    alerts = _detect_alerts(active_opps)
    msg = _format_pulse_message(summary, active_opps, alerts, purged)

    if target_channel:
        slack_client.chat_postMessage(
            channel=target_channel,
            text=msg,
            unfurl_links=False,
            unfurl_media=False,
        )
        logger.info(f"Daily pulse posted to {target_channel}")

    return msg


def _detect_alerts(active_opps: list) -> list:
    alerts = []
    now = datetime.utcnow()

    for opp in active_opps:
        opp_id = opp["opp_id"]
        account_name = opp.get("account_name") or "Unknown"

        if opp.get("prev_ari") and opp.get("prev_ari") != opp.get("ari_category"):
            alerts.append(
                {
                    "type": "ARI_CHANGED",
                    "opp_id": opp_id,
                    "account": account_name,
                    "message": (
                        f"ARI changed: {opp.get('prev_ari')} -> {opp.get('ari_category')}"
                    ),
                }
            )

        stage_order = ["Initiate", "Qualify", "Demonstrate", "Negotiate", "Deliver"]
        prev_stage = opp.get("prev_stage")
        curr_stage = opp.get("opp_stage")
        if prev_stage and curr_stage and prev_stage in stage_order and curr_stage in stage_order:
            if stage_order.index(curr_stage) < stage_order.index(prev_stage):
                alerts.append(
                    {
                        "type": "STAGE_REGRESSED",
                        "opp_id": opp_id,
                        "account": account_name,
                        "message": f"Stage regressed: {prev_stage} -> {curr_stage}",
                    }
                )

        close_date = opp.get("close_date")
        updated_at = opp.get("updated_at")
        if close_date and updated_at:
            try:
                close_dt = datetime.strptime(str(close_date)[:10], "%Y-%m-%d")
                updated_dt = datetime.fromisoformat(str(updated_at).replace("Z", "+00:00"))
                updated_dt_naive = updated_dt.replace(tzinfo=None)
                days_to_close = (close_dt - now).days
                days_since_update = (now - updated_dt_naive).days
                if days_to_close < 30 and days_since_update > 7:
                    alerts.append(
                        {
                            "type": "STALE_ACCOUNT",
                            "opp_id": opp_id,
                            "account": account_name,
                            "message": (
                                f"Closes in {days_to_close} days, "
                                f"no update for {days_since_update} days"
                            ),
                        }
                    )
            except Exception as e:
                logger.warning(f"Date parsing error for {opp_id}: {e}")

        if opp.get("prev_atr") and opp.get("atr"):
            prev_atr = float(opp["prev_atr"])
            curr_atr = float(opp["atr"])
            if prev_atr > 0 and ((curr_atr - prev_atr) / prev_atr) > 0.20:
                alerts.append(
                    {
                        "type": "ATR_INCREASED",
                        "opp_id": opp_id,
                        "account": account_name,
                        "message": (
                            f"ATR increased >20%: ${prev_atr:,.0f} -> ${curr_atr:,.0f}"
                        ),
                    }
                )

    return alerts


def _format_pulse_message(
    summary: dict, active_opps: list, alerts: list, purged: int
) -> str:
    lines = [
        f":chart_with_upwards_trend: *Daily Pulse* — {datetime.utcnow().strftime('%B %d, %Y')}",
        "",
    ]

    by_cloud = summary.get("by_cloud", [])
    if by_cloud:
        lines.append("*Active Strategic Opps:*")
        for cloud_data in by_cloud:
            cloud = cloud_data.get("cloud") or "Unknown"
            total = cloud_data.get("total") or 0
            high = cloud_data.get("high_ari") or 0
            medium = cloud_data.get("medium_ari") or 0
            total_atr = cloud_data.get("total_atr") or 0
            acting = cloud_data.get("acting") or 0
            lines.append(
                f"- {cloud}: {total} opps ({high} High, {medium} Medium) | "
                f"${total_atr:,.0f} ATR | {acting} in ACTING state"
            )
        lines.append("")

    if alerts:
        lines.append(f":warning: *{len(alerts)} Alert(s):*")
        for alert in alerts:
            lines.append(f"- {alert['account']}: {alert['message']}")
        lines.append("")
    else:
        lines.append(":white_check_mark: *No alerts — all opps stable*")
        lines.append("")

    if purged > 0:
        lines.append(f"_Purged {purged} closed/expired opp(s)_")

    return "\n".join(lines)

