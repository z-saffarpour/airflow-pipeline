"""Parse-time configuration without metadata-database round-trips.

Airflow DAG files must not call ``Variable.get()`` at import time: each call
hits the metadata DB and can push DagBag import over
``dagbag_import_timeout`` (30s by default).

This helper reads Airflow's official environment override
``AIRFLOW_VAR_<KEY>`` and otherwise returns the provided default.

UI/DB Variables are intentionally not read during parse. Set the matching
environment variable, or edit the DAG file.
"""
from __future__ import annotations

import os
from typing import Optional


def _env_raw(key: str) -> Optional[str]:
    return os.environ.get(f"AIRFLOW_VAR_{key.upper()}")


def parse_str(key: str, default: str) -> str:
    """Return ``AIRFLOW_VAR_<KEY>`` or ``default``."""
    raw = _env_raw(key)
    if raw is None:
        return default
    return raw


def parse_int(key: str, default: int) -> int:
    """Return ``AIRFLOW_VAR_<KEY>`` parsed as int, or ``default``."""
    raw = _env_raw(key)
    if raw is None or raw == "":
        return default
    return int(raw)


def parse_optional_str(key: str, default: str = "") -> Optional[str]:
    """Like ``parse_str``, but blank values become ``None``."""
    value = parse_str(key, default)
    return value or None
