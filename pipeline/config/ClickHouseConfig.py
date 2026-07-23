"""
Configuration dataclass for ClickHouse optimization.
Immutable configuration holder.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class ClickHouseConfig:    
    database: str
    table_name: str
    
    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.database}.{self.table_name}"

