"""
log_utils.py
Simple logging utilities
"""
import sys
from datetime import datetime


def log_debug(msg: str):
    """Print debug message with timestamp"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)


def log_error(msg: str):
    """Print error message"""
    print(f"❌ {msg}", file=sys.stderr, flush=True)
