# pipeline/config/AuditConfig.py

"""Audit logging configuration and event type definitions."""

from enum import Enum
import os


class EventType(str, Enum):
    """Standard event types for audit logging."""
    CONN_VALIDATED = "conn_validated"
    STORE_LIST_RETRIEVED = "store_list_retrieved"
    DAG_COMPLETED = "dag_completed"
    METADATA_GENERATED = "metadata_generated"
    DATA_TRANSFER_ORCHESTRATOR = "data_transfer_orchestrator"
    KAFKA_TOPIC_CONFIGURED = "kafka_topic_configured"
    KAFKA_PRODUCE = "kafka_produce"
    KAFKA_CONSUME = "kafka_consume"
    PROCESSING_SERVER = "process_server"
    DB_QUERY = "db_query"
    CHUNK_PROCESSING = "chunk_processing"
    VALIDATION = "validation"
    FILE_OPERATION = "file_operation"
    REPLICATION_SCRIPT_GENERATED = "replication_script_generated"
    REPLICATION_RECONCILE = "replication_reconcile"
    REPLICATION_RECONCILE_SUMMARY = "replication_reconcile_summary"
    SYNC_DAG_TRIGGERED = "sync_dag_triggered"


class EventStatus(str, Enum):
    """Event status values."""
    STARTED = "started"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    WARNING = "warning"


AUDIT_CONFIG = {
    'log_dir': os.getenv('AUDIT_LOG_DIR', '/opt/airflow/logs/audit'),
    'retention_days': int(os.getenv('AUDIT_RETENTION_DAYS', '30')),
}
