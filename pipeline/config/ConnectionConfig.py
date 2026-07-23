"""
Configuration dataclass for database and message broker connections.
Immutable configuration holder.
"""
from dataclasses import dataclass
from typing import Dict, Any


@dataclass(frozen=True)
class ConnectionConfig:
    """Configuration for database and message broker connections. Immutable."""
    mssql_conn_id: str = None # "mssql_default"
    kafka_conn_id: str = None # "kafka_default"
    clickhouse_conn_id: str = None # "clickhouse_default"

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'mssql_conn_id': self.mssql_conn_id,
            'kafka_conn_id': self.kafka_conn_id,
            'clickhouse_conn_id': self.clickhouse_conn_id,
        }
