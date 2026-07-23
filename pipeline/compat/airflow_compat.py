"""
Centralized Airflow version compatibility module.
Replaces duplicated try/except blocks across all DAG files.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

def _build_get_connection():
    """Build get_connection function based on available Airflow version."""
    try:
        from airflow.sdk import Connection  # type: ignore # Airflow 3.x SDK
        logger.debug("airflow_compat: using airflow.sdk.Connection")
        return lambda conn_id: Connection.get(conn_id)
    except ImportError:
        pass

    try:
        from airflow.models import Connection  # type: ignore # Airflow 2.8+
        logger.debug("airflow_compat: using airflow.models.Connection")
        return lambda conn_id: Connection.get_connection_from_secrets(conn_id)
    except (ImportError, AttributeError):
        pass

    from airflow.hooks.base import BaseHook  # type: ignore # Fallback for older versions
    logger.debug("airflow_compat: using BaseHook fallback")
    return lambda conn_id: BaseHook.get_connection(conn_id)

# Build once at import time
get_connection = _build_get_connection()