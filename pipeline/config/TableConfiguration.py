"""
Configuration dataclass for a single table transfer.
Immutable configuration holder.
"""
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple, ClassVar

@dataclass(frozen=True)
class TableConfiguration:
    """Configuration for a single table transfer. Immutable."""
    table_name: str
    primary_key_column: str
    order_by_column: Optional[str]
    # batch_size: int = 50000
    columns: Optional[Tuple[str, ...]] = None  # Tuple for immutability
    date_column: Optional[str] = None  # Optional: if None, reads all records without date filter
    date_column_type: str = 'int'  # 'int' or 'date' - determines how date_key is passed to query

    _VALID_DATE_COLUMN_TYPES: ClassVar[frozenset] = frozenset({"int", "date", "datetime"})
          
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'table_name': self.table_name,
            'primary_key_column': self.primary_key_column,
            'columns': self.columns,
            'date_column': self.date_column,
            'date_column_type': self.date_column_type,
            'order_by_column': self.order_by_column,
            'batch_size': self.batch_size,
            'kafka_topic': self.kafka_topic,
            'clickhouse_database': self.clickhouse_database,
            'clickhouse_table_name': self.clickhouse_table_name,
        }
        
    def __post_init__(self):
        # Validate remaining fields
        if not self.table_name or not self.table_name.strip():
            raise ValueError("'table_name' is required and must not be empty.")
        
        if not self.primary_key_column or not self.primary_key_column.strip():
            raise ValueError("'primary_key_column' must not be empty.")
        
        # Validate date_column_type when date_column is provided
        if self.date_column and self.date_column_type not in self._VALID_DATE_COLUMN_TYPES:
            raise ValueError(
                f"'date_column_type' must be one of "
                f"{sorted(self._VALID_DATE_COLUMN_TYPES)}, "
                f"got {self.date_column_type!r}."
            )