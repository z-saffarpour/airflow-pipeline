"""
Connection Validation
=====================
Standardized validation functions for Kafka and SQL Server connections.
Returns structured validation results suitable for XCom.
    
Author: Senior Data Engineer
Version: 3.0
"""

import logging
from datetime import datetime
from dataclasses import dataclass, asdict
from airflow.exceptions import AirflowException # type: ignore

from pipeline.database.ConnectionFactory import ConnectionFactory
from pipeline.database.ClickHouseConnectionFactory import ClickHouseConnectionFactory
from pipeline.kafka.KafkaConnectionFactory import KafkaConnectionFactory
# ============================================================================
# LOGGING
# ============================================================================

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Structured validation result for XCom serialization."""
    status: str
    conn_id: str
    timestamp: str
    details: dict = None
    
    def to_dict(self) -> dict:
        return asdict(self)
    
# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_kafka_conn(conn_id: str) -> dict:
    """
    Validate a Kafka connection by listing cluster brokers.

    This function is intentionally side-effect free beyond the network call —
    it does not create topics or produce/consume messages.

    Used by:
    - make_validate_kafka_task() in sqlserver_kafka_query_sync.py
    - make_validate_kafka_task() in sqlserver_kafka_table_sync.py

    Args:
        conn_id: Airflow Connection ID for the Kafka cluster.

    Returns:
        dict with keys: status, conn_id, timestamp

    Raises:
        AirflowException: If the cluster is unreachable or returns no brokers.

    Example:
        result = validate_kafka_conn("kafka_default")
        # {"status": "ok", "conn_id": "kafka_default", "timestamp": "2026-03-16T15:50:14Z"}
    """
    logger.info("Validating Kafka connection: %s", conn_id)
    
    try:
        factory = KafkaConnectionFactory(conn_id=conn_id)
        cluster = factory.get_cluster_info(timeout=10)

        if cluster["broker_count"] == 0:
            raise AirflowException(f"No brokers found for {conn_id}")

        brokers_count = cluster["broker_count"]
        result = ValidationResult(
            status="ok",
            conn_id=conn_id,
            timestamp=datetime.now().isoformat(),
            details={"brokers_count": brokers_count}
        )
        
        logger.info("Kafka validated: %s (%d brokers)", conn_id, brokers_count)
        return result.to_dict()
    
    except Exception as exc:
        logger.exception("Kafka validation failed: %s", conn_id)
        raise AirflowException(f"Kafka validation failed for {conn_id}: {exc}")


def validate_mssql_conn(conn_id: str, hook_class) -> dict:
    """
    Validate a SQL Server connection by executing a simple query.

    Used by:
    - make_validate_mssql_task() in sqlserver_kafka_query_sync.py
    - make_validate_mssql_task() in sqlserver_kafka_table_sync.py

    Args:
        conn_id: Airflow Connection ID for the SQL Server instance.
        hook_class: The MsSqlHook class to use (e.g., SafeMsSqlHook).

    Returns:
        dict with keys: status, conn_id, timestamp

    Raises:
        AirflowException: If the connection fails or query execution fails.

    Example:
        result = validate_mssql_conn("mssql_default")
        #  {"status": "ok", "conn_id": "mssql_default", "timestamp": "..."}
    """
    logger.info("Validating SQL Server connection: %s", conn_id)
    
    try:
        factory = ConnectionFactory(conn_id = conn_id)
        result = factory.test_connection()
               
        if result == False:
            raise AirflowException(f"SQL Server test query returned no results: {conn_id}")
        
        validation_result = ValidationResult(
            status="ok",
            conn_id=conn_id,
            timestamp=datetime.now().isoformat(),
        )
        
        logger.info("SQL Server validated: %s", conn_id)
        return validation_result.to_dict()
    
    except Exception as exc:
        logger.exception("SQL Server validation failed: %s", conn_id)
        raise AirflowException(f"SQL Server validation failed for {conn_id}: {exc}")



def validate_mssql_conn(conn_id: str) -> dict:
    """
    Validate a SQL Server connection by executing a simple query.

    Used by:
    - make_validate_mssql_task() in sqlserver_kafka_query_sync.py
    - make_validate_mssql_task() in sqlserver_kafka_table_sync.py

    Args:
        conn_id: Airflow Connection ID for the SQL Server instance.
        hook_class: The MsSqlHook class to use (e.g., SafeMsSqlHook).

    Returns:
        dict with keys: status, conn_id, timestamp

    Raises:
        AirflowException: If the connection fails or query execution fails.

    Example:
        result = validate_mssql_conn("mssql_default")
        #  {"status": "ok", "conn_id": "mssql_default", "timestamp": "..."}
    """
    logger.info("Validating SQL Server connection: %s", conn_id)
    
    try:
        factory = ConnectionFactory(conn_id = conn_id)
        result = factory.test_connection()
               
        if result == False:
            raise AirflowException(f"SQL Server test query returned no results: {conn_id}")
        
        validation_result = ValidationResult(
            status="ok",
            conn_id=conn_id,
            timestamp=datetime.now().isoformat(),
        )
        
        logger.info("SQL Server validated: %s", conn_id)
        return validation_result.to_dict()
    
    except Exception as exc:
        logger.exception("SQL Server validation failed: %s", conn_id)
        raise AirflowException(f"SQL Server validation failed for {conn_id}: {exc}")

        
def validate_clickhouse_conn(conn_id: str) -> dict:
    """
    Validate a ClickHouse connection by executing a simple query.

    Used by:
    - make_validate_clickhouse_task() in sqlserver_kafka_query_sync.py
    - make_validate_clickhouse_task() in sqlserver_kafka_table_sync.py

    Args:
        conn_id: Airflow Connection ID for the ClickHouse instance.

    Returns:
        dict with keys: status, conn_id, timestamp, server_version

    Raises:
        AirflowException: If the connection fails or query execution fails.

    Example:
        result = validate_clickhouse_conn("mssql_default")
        #  {"status": "ok", "conn_id": "mssql_default", "timestamp": "..."}
    """
    logger.info("Validating ClickHouse connection: %s", conn_id)
    
    try:
        factory = ClickHouseConnectionFactory(conn_id = conn_id)
        result = factory.test_connection()
               
        if result == False:
            raise AirflowException(f"ClickHouse test query returned no results: {conn_id}")
        
        validation_result = ValidationResult(
            status="ok",
            conn_id=conn_id,
            timestamp=datetime.now().isoformat(),
        )
        
        logger.info("ClickHouse validated: %s", conn_id)
        return validation_result.to_dict()
    
    except Exception as exc:
        logger.exception("ClickHouse validation failed: %s", conn_id)
        raise AirflowException(f"ClickHouse validation failed for {conn_id}: {exc}")
