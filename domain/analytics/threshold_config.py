"""
domain/analytics/threshold_config.py
Threshold loader for cloud-specific adoption health bands.
"""

import os

import yaml

_DEFAULTS = {"green": 70.0, "yellow": 30.0, "red": 0.0}
_cache: dict = {}


def get_thresholds(cloud_family: str) -> dict:
    """
    Loads thresholds from config/thresholds.yaml.
    Falls back to defaults if cloud not configured.
    """
    global _cache
    if not _cache:
        config_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/thresholds.yaml")
        )
        with open(config_path, "r", encoding="utf-8") as f:
            _cache = (yaml.safe_load(f) or {}).get("clouds", {})

    cloud_config = _cache.get(cloud_family, _DEFAULTS)
    return {
        "green": float(cloud_config.get("green", _DEFAULTS["green"])),
        "yellow": float(cloud_config.get("yellow", _DEFAULTS["yellow"])),
        "red": float(cloud_config.get("red", _DEFAULTS["red"])),
    }


def reload_thresholds():
    """Clears cache so next read reloads thresholds.yaml."""
    global _cache
    _cache = {}
