"""
log_utils.py
Simple logging utilities
"""
import json
import sys
import time
from datetime import datetime


def log_structured(event: str, **kwargs) -> None:
    """
    Emit one JSON log line for observability (Datadog / Splunk / CloudWatch).

    Always includes ``timestamp`` (UTC ISO8601), ``level`` (default ``info``), and ``event``.
    Extra fields come from ``kwargs``; use ``level="error"`` to override.

    Usage::

        log_structured("llm_call", status="ok", latency_ms=320, model="claude-sonnet-4-5")
        log_structured("sf_limit_exceeded", error="REQUEST_LIMIT_EXCEEDED")
    """
    level = kwargs.pop("level", "info")
    payload = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": level,
        "event": event,
        **kwargs,
    }
    print(json.dumps(payload, default=str), flush=True)


def log_debug(msg: str):
    """Print debug message with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def log_error(msg: str):
    """Print error message"""
    print(f"❌ {msg}", file=sys.stderr, flush=True)
