"""
Configuration dataclass for database and message broker connections.
Immutable configuration holder.
"""
from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class ConnectionConfig:
    """Configuration for database and message broker connections. Immutable."""
    mssql_conn_id: str = None # "mssql_default"  # source, or sole MSSQL when only one is needed
    mssql_target_conn_id: str = None # "mssql_target_default" (MSSQL→MSSQL fixed-target path)
    mysql_conn_id: str = None # "mysql_default"
    postgres_conn_id: str = None # "postgres_default"
    mongo_conn_id: str = None # "mongo_default"
    kafka_conn_id: str = None # "kafka_default"
    clickhouse_conn_id: str = None # "clickhouse_default"

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'mssql_conn_id': self.mssql_conn_id,
            'mssql_target_conn_id': self.mssql_target_conn_id,
            'mysql_conn_id': self.mysql_conn_id,
            'postgres_conn_id': self.postgres_conn_id,
            'mongo_conn_id': self.mongo_conn_id,
            'kafka_conn_id': self.kafka_conn_id,
            'clickhouse_conn_id': self.clickhouse_conn_id,
        }
