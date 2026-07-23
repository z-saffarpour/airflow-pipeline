"""
Configuration dataclass for ClickHouse optimization.
Immutable configuration holder.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ClickHouseOptimizationConfig:
    """Configuration for ClickHouse table optimization. Immutable."""
    
    database: str
    table_name: str
    cluster_name: Optional[str]= None
    partition_column: Optional[str] = None  # Column used for partitioning
    partition_format: str = 'YYYYMMDD'  # Format of partition values
    final: bool = True  # Perform FINAL merge
    deduplicate: bool = True  # Remove duplicate rows
    optimize_timeout_seconds: int = 3600  # Timeout for optimization
    
    @property
    def full_table_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.database}.{self.table_name}"
    
    def get_partition_value(self, execution_date: str) -> Optional[str]:
        """
        Convert execution_date to partition value based on format.
        
        Args:
            execution_date: Date in YYYYMMDD format
            
        Returns:
            Partition value or None if no partition column
        """
        if not self.partition_column:
            return None
        
        # For YYYYMMDD format, return as-is
        if self.partition_format == 'YYYYMMDD':
            return execution_date
        # For YYYYMM format, truncate
        elif self.partition_format == 'YYYYMM':
            return execution_date[:6]
        # For YYYY format
        elif self.partition_format == 'YYYY':
            return execution_date[:4]
        
        return execution_date
