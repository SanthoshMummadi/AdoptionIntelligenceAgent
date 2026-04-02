import os

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()


def log_debug(msg: str):
    if LOG_LEVEL == "DEBUG":
        print(f"[DEBUG] {msg}", flush=True)


def log_error(msg: str):
    print(f"[ERROR] {msg}", flush=True)
