# dags/core/connection_utils.py

"""
Airflow Connection Utilities
=============================
Provides version-agnostic connection resolution across Airflow 2.x and 3.x.
    
Author: Senior Data Engineer
Version: 3.0
"""

import json
import logging
from airflow.exceptions import AirflowException # type: ignore

# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# CONNECTION COMPATIBILITY LAYER
# ============================================================================

try:
    from airflow.sdk import Connection # type: ignore
    
    def get_connection(conn_id: str):
        """Resolve an Airflow Connection using the Airflow 3.x SDK."""
        return Connection.get(conn_id)
except ImportError:
    try:
        from airflow.models import Connection # type: ignore
        
        def get_connection(conn_id: str):
            """Resolve an Airflow Connection using airflow.models (2.8+)."""
            return Connection.get_connection_from_secrets(conn_id)
    except (ImportError, AttributeError):
        from airflow.hooks.base import BaseHook # type: ignore
        
        def get_connection(conn_id: str):
            """Resolve an Airflow Connection using BaseHook (legacy fallback)."""
            return BaseHook.get_connection(conn_id)


def load_connection_extra(conn) -> dict:
    """
    Safely parse connection extra JSON field.
    
    Args:
        conn: Airflow Connection object
        
    Returns:
        Parsed extra dict (empty dict if extra is None/invalid)
        
    Raises:
        AirflowException: If extra contains invalid JSON
    """
    try:
        return json.loads(conn.extra or "{}")
    except json.JSONDecodeError as exc:
        raise AirflowException(
            f"Invalid JSON in connection extra for {conn.conn_id}: {exc}"
        )
