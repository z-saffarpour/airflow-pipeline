# dags/core/kafka_utils.py

"""
Kafka Utilities
===============
Kafka-specific helpers for broker resolution, AdminClient creation, and topic operations.
    
Author: Senior Data Engineer
Version: 3.0
"""

import logging
from confluent_kafka.admin import AdminClient # type: ignore
from pipeline.utils.connection_utils import get_connection, load_connection_extra

# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)

# ============================================================================
# KAFKA HELPERS
# ============================================================================

def get_kafka_brokers(conn_id: str = "kafka_default") -> str:
    """
    Resolve Kafka bootstrap servers from an Airflow Connection.

    Resolution order:
    1. extra.bootstrap_servers  — explicit multi-broker string
    2. {host}:{port}            — single broker fallback (port defaults to 9092)

    Args:
        conn_id: Airflow Connection ID for the Kafka cluster.

    Returns:
        Bootstrap servers string suitable for bootstrap.servers config key.
        Example: "broker1:9092,broker2:9092"

    Raises:
        AirflowException: If the connection cannot be resolved.
    """
    conn = get_connection(conn_id)
    extra = load_connection_extra(conn)
    return extra.get("bootstrap_servers") or f"{conn.host}:{conn.port or 9092}"


def build_kafka_admin_client(conn_id: str) -> AdminClient:
    """
    Build a confluent-kafka AdminClient from an Airflow Connection.

    Supported extra fields on the Airflow Connection:
    - bootstrap_servers  : override host:port with a full broker list
    - client_id          : Kafka client identifier (default: "airflow")
    - security_protocol  : e.g. "SASL_SSL"
    - sasl_mechanism     : e.g. "PLAIN" or "SCRAM-SHA-256"
    - sasl_username      : SASL username (required when sasl_mechanism is set)
    - sasl_password      : SASL password (required when sasl_mechanism is set)

    Args:
        conn_id: Airflow Connection ID for the Kafka cluster.

    Returns:
        A configured confluent_kafka.admin.AdminClient instance.

    Raises:
        AirflowException: If the connection cannot be resolved or client creation fails.
    """
    conn = get_connection(conn_id)
    extra = load_connection_extra(conn)

    kafka_conf = {
        "bootstrap.servers": extra.get("bootstrap_servers") or f"{conn.host}:{conn.port or 9092}",
        "client.id": extra.get("client_id", "airflow"),
    }

    if extra.get("security_protocol"):
        kafka_conf["security.protocol"] = extra["security_protocol"]

    if extra.get("sasl_mechanism"):
        kafka_conf["sasl.mechanism"] = extra["sasl_mechanism"]
        kafka_conf["sasl.username"] = extra["sasl_username"]
        kafka_conf["sasl.password"] = extra["sasl_password"]

    return AdminClient(kafka_conf)
