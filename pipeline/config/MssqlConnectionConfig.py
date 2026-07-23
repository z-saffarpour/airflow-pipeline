from dataclasses import dataclass

@dataclass(frozen=True)
class MssqlConnectionConfig:
    """
    Immutable configuration for a single MSSQL connection validation.

    Attributes:
        conn_id      : Airflow connection ID
        description  : Human-readable description for logs and audit
    """
    conn_id:     str
    description: str