import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DB_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "bot_history.db")
)

# Strategic opp thresholds
MIN_ATR = 500_000
MAX_TRACKING_MONTHS = 18

# Valid states
STATES = ["IDENTIFIED", "DISCUSSING", "ACTING", "RESOLVED"]

# Auto-purge outcomes
CLOSED_OUTCOMES = ["Won", "Lost", "Renewed", "Churned", "Dead"]


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def setup_tracking_tables():
    """
    CREATE TABLE IF NOT EXISTS — safe to call on every startup.
    """
    conn = _get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS account_tracking (
            opp_id          TEXT PRIMARY KEY,
            account_id      TEXT NOT NULL,
            account_name    TEXT,
            cloud           TEXT,
            is_strategic    INTEGER DEFAULT 1,
            ari_category    TEXT,
            ari_probability REAL,
            atr             REAL,
            opp_stage       TEXT,
            close_date      TEXT,
            health_score    INTEGER,
            prev_ari        REAL,
            prev_atr        REAL,
            prev_stage      TEXT,
            state           TEXT DEFAULT 'IDENTIFIED',
            outcome         TEXT,
            slack_channel   TEXT,
            canvas_url      TEXT,
            gm_review_batch TEXT,
            created_at      TEXT DEFAULT (datetime('now')),
            updated_at      TEXT DEFAULT (datetime('now')),
            expires_at      TEXT,
            closed_at       TEXT
        )
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS tracking_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            opp_id      TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            old_value   TEXT,
            new_value   TEXT,
            timestamp   TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (opp_id) REFERENCES account_tracking(opp_id)
        )
        """
    )

    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_tracking_account ON account_tracking(account_id)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_tracking_cloud ON account_tracking(cloud, is_strategic)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_tracking_state ON account_tracking(state, outcome)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_events_opp ON tracking_events(opp_id, event_type)"
    )

    conn.commit()
    conn.close()
    logger.info("✓ Tracking tables ready")


def is_strategic(opp: dict) -> bool:
    """
    Returns True if opp qualifies for strategic tracking.
    Rules:
    - ATR >= $500K
    - Close date within 18 months
    - Not already closed
    """
    try:
        atr = float(opp.get("atr") or opp.get("Amount") or 0)
        close_date = opp.get("close_date") or opp.get("CloseDate") or ""
        is_closed = bool(opp.get("is_closed") or opp.get("IsClosed"))

        if is_closed:
            return False
        if atr < MIN_ATR:
            return False
        if close_date:
            close_dt = datetime.strptime(close_date[:10], "%Y-%m-%d")
            if close_dt > datetime.now() + timedelta(days=MAX_TRACKING_MONTHS * 30):
                return False
        return True
    except Exception as e:
        logger.warning(f"is_strategic check failed: {e}")
        return False


def upsert_tracking(
    opp: dict,
    canvas_url: str = None,
    gm_review_batch: str = None,
    slack_channel: str = None,
) -> bool:
    """
    Add or update an opp in tracking.
    Idempotent — safe to call multiple times.
    Returns True if inserted, False if updated/skipped.
    """
    if not is_strategic(opp):
        logger.info(f"Opp {opp.get('opp_id')} not strategic — skipping tracking")
        return False

    opp_id = opp.get("opp_id") or opp.get("Id")
    if not opp_id:
        logger.warning("upsert_tracking: no opp_id provided")
        return False

    now = datetime.utcnow().isoformat()
    expires_at = (
        datetime.utcnow() + timedelta(days=MAX_TRACKING_MONTHS * 30)
    ).isoformat()

    conn = _get_conn()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT opp_id, ari_category, atr, opp_stage FROM account_tracking WHERE opp_id = ?",
        (opp_id,),
    )
    existing = cursor.fetchone()

    if existing:
        cursor.execute(
            """
            UPDATE account_tracking SET
                account_name    = ?,
                ari_category    = ?,
                ari_probability = ?,
                atr             = ?,
                opp_stage       = ?,
                close_date      = ?,
                prev_ari        = ari_category,
                prev_atr        = atr,
                prev_stage      = opp_stage,
                updated_at      = ?,
                canvas_url      = COALESCE(?, canvas_url),
                slack_channel   = COALESCE(?, slack_channel),
                gm_review_batch = COALESCE(?, gm_review_batch)
            WHERE opp_id = ?
            """,
            (
                opp.get("account_name"),
                opp.get("ari_category"),
                opp.get("ari_probability"),
                opp.get("atr"),
                opp.get("opp_stage"),
                opp.get("close_date"),
                now,
                canvas_url,
                slack_channel,
                gm_review_batch,
                opp_id,
            ),
        )

        _log_changes(cursor, opp_id, existing, opp, now)

        conn.commit()
        conn.close()
        logger.info(f"Updated tracking for opp {opp_id}")
        return False

    cursor.execute(
        """
        INSERT INTO account_tracking (
            opp_id, account_id, account_name, cloud,
            is_strategic, ari_category, ari_probability,
            atr, opp_stage, close_date,
            state, canvas_url, gm_review_batch,
            slack_channel, created_at, updated_at, expires_at
        ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, 'IDENTIFIED', ?, ?, ?, ?, ?, ?)
        """,
        (
            opp_id,
            opp.get("account_id"),
            opp.get("account_name"),
            opp.get("cloud"),
            opp.get("ari_category"),
            opp.get("ari_probability"),
            opp.get("atr"),
            opp.get("opp_stage"),
            opp.get("close_date"),
            canvas_url,
            gm_review_batch,
            slack_channel,
            now,
            now,
            expires_at,
        ),
    )

    cursor.execute(
        """
        INSERT INTO tracking_events (opp_id, event_type, new_value, timestamp)
        VALUES (?, 'TRACKING_STARTED', ?, ?)
        """,
        (
            opp_id,
            json.dumps({"ari": opp.get("ari_category"), "atr": opp.get("atr")}),
            now,
        ),
    )

    conn.commit()
    conn.close()
    logger.info(f"Started tracking opp {opp_id}")
    return True


def _log_changes(cursor, opp_id: str, existing, new_opp: dict, now: str):
    """Log meaningful changes to tracking_events."""
    if existing["ari_category"] != new_opp.get("ari_category"):
        cursor.execute(
            """
            INSERT INTO tracking_events (opp_id, event_type, old_value, new_value, timestamp)
            VALUES (?, 'ARI_CHANGED', ?, ?, ?)
            """,
            (opp_id, existing["ari_category"], new_opp.get("ari_category"), now),
        )
        logger.info(
            f"ARI changed for {opp_id}: {existing['ari_category']} -> {new_opp.get('ari_category')}"
        )

    if existing["opp_stage"] != new_opp.get("opp_stage"):
        cursor.execute(
            """
            INSERT INTO tracking_events (opp_id, event_type, old_value, new_value, timestamp)
            VALUES (?, 'STAGE_MOVED', ?, ?, ?)
            """,
            (opp_id, existing["opp_stage"], new_opp.get("opp_stage"), now),
        )
        logger.info(
            f"Stage changed for {opp_id}: {existing['opp_stage']} -> {new_opp.get('opp_stage')}"
        )

    old_atr = float(existing["atr"] or 0)
    new_atr = float(new_opp.get("atr") or 0)
    if old_atr > 0 and abs(new_atr - old_atr) / old_atr > 0.20:
        cursor.execute(
            """
            INSERT INTO tracking_events (opp_id, event_type, old_value, new_value, timestamp)
            VALUES (?, 'ATR_CHANGED', ?, ?, ?)
            """,
            (opp_id, str(old_atr), str(new_atr), now),
        )
        logger.info(f"ATR changed >20% for {opp_id}: {old_atr} -> {new_atr}")


def transition_state(opp_id: str, new_state: str) -> bool:
    """
    Transition opp to a new state.
    Valid transitions: IDENTIFIED -> DISCUSSING -> ACTING -> RESOLVED
    """
    if new_state not in STATES:
        logger.warning(f"Invalid state: {new_state}")
        return False

    now = datetime.utcnow().isoformat()
    conn = _get_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT state FROM account_tracking WHERE opp_id = ?", (opp_id,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        return False

    old_state = row["state"]
    cursor.execute(
        "UPDATE account_tracking SET state = ?, updated_at = ? WHERE opp_id = ?",
        (new_state, now, opp_id),
    )
    cursor.execute(
        """
        INSERT INTO tracking_events (opp_id, event_type, old_value, new_value, timestamp)
        VALUES (?, 'STATE_CHANGED', ?, ?, ?)
        """,
        (opp_id, old_state, new_state, now),
    )

    conn.commit()
    conn.close()
    logger.info(f"State transition for {opp_id}: {old_state} -> {new_state}")
    return True


def log_outcome(opp_id: str, outcome: str) -> bool:
    """
    Log final outcome and mark for removal.
    Triggers auto-purge on next cron run.
    """
    if outcome not in CLOSED_OUTCOMES:
        logger.warning(f"Invalid outcome: {outcome}")
        return False

    now = datetime.utcnow().isoformat()
    conn = _get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE account_tracking SET
            outcome    = ?,
            state      = 'RESOLVED',
            closed_at  = ?,
            updated_at = ?
        WHERE opp_id = ?
        """,
        (outcome, now, now, opp_id),
    )
    cursor.execute(
        """
        INSERT INTO tracking_events (opp_id, event_type, new_value, timestamp)
        VALUES (?, 'OUTCOME_LOGGED', ?, ?)
        """,
        (opp_id, outcome, now),
    )

    conn.commit()
    conn.close()
    logger.info(f"Outcome logged for {opp_id}: {outcome}")
    return True


def purge_closed() -> int:
    """
    Remove closed opps and expired tracking records.
    Called at start of every weekly cron.
    Returns count of purged records.
    """
    now = datetime.utcnow().isoformat()
    conn = _get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        DELETE FROM account_tracking
        WHERE outcome IN ('Won', 'Lost', 'Renewed', 'Churned', 'Dead')
        OR expires_at < ?
        """,
        (now,),
    )

    purged = cursor.rowcount
    conn.commit()
    conn.close()

    if purged > 0:
        logger.info(f"Purged {purged} closed/expired tracking records")
    return purged


def get_active_tracked(cloud: str = None) -> list:
    """
    Returns all active tracked opps (not closed/expired).
    Optionally filter by cloud.
    """
    conn = _get_conn()
    cursor = conn.cursor()

    if cloud:
        cursor.execute(
            """
            SELECT * FROM account_tracking
            WHERE outcome IS NULL
            AND expires_at > datetime('now')
            AND cloud = ?
            ORDER BY ari_category DESC, close_date ASC
            """,
            (cloud,),
        )
    else:
        cursor.execute(
            """
            SELECT * FROM account_tracking
            WHERE outcome IS NULL
            AND expires_at > datetime('now')
            ORDER BY ari_category DESC, close_date ASC
            """
        )

    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def get_tracking_summary() -> dict:
    """
    Returns summary stats for weekly pulse.
    """
    conn = _get_conn()
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            cloud,
            COUNT(*) as total,
            SUM(CASE WHEN ari_category = 'High' THEN 1 ELSE 0 END) as high_ari,
            SUM(CASE WHEN ari_category = 'Medium' THEN 1 ELSE 0 END) as medium_ari,
            SUM(atr) as total_atr,
            SUM(CASE WHEN state = 'ACTING' THEN 1 ELSE 0 END) as acting
        FROM account_tracking
        WHERE outcome IS NULL
        AND expires_at > datetime('now')
        GROUP BY cloud
        """
    )

    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return {"by_cloud": rows}
