# dags/core/kafka_utils.py

"""
Kafka Utilities
===============
Thin helpers over KafkaConnectionFactory for broker resolution and AdminClient.

Prefer KafkaConnectionFactory directly in new code.

Author: Senior Data Engineer
Version: 3.1
"""

import logging
from confluent_kafka.admin import AdminClient  # type: ignore

from pipeline.kafka.KafkaConnectionFactory import KafkaConnectionFactory

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
    return KafkaConnectionFactory(conn_id).get_bootstrap_servers()


def build_kafka_admin_client(conn_id: str) -> AdminClient:
    """
    Build a confluent-kafka AdminClient from an Airflow Connection.

    Delegates to KafkaConnectionFactory.create_admin_client.

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
        KafkaConnectionError: If the connection cannot be resolved or client creation fails.
    """
    return KafkaConnectionFactory(conn_id).create_admin_client()
