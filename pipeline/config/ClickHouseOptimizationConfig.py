"""
Configuration dataclass for ClickHouse optimization.
Immutable configuration holder.
"""
from dataclasses import dataclass
from typing import Optional

from pipeline.utils.persian_calendar import (
    to_persian_date,
    to_persian_date_key,
    to_persian_year,
    to_persian_year_month,
)


# Supported partition_format values:
#   Gregorian: YYYYMMDD, YYYYMM, YYYY
#   Persian/Jalali: PERSIAN_YYYYMMDD, PERSIAN_YYYYMM, PERSIAN_YYYY, PERSIAN_YYYY/MM/DD
#                   (aliases: JALALI_*, SHAMSI_*)
_PERSIAN_ALIASES = {
    "PERSIAN_YYYYMMDD": "PERSIAN_YYYYMMDD",
    "PERSIAN_YYYYMM": "PERSIAN_YYYYMM",
    "PERSIAN_YYYY": "PERSIAN_YYYY",
    "PERSIAN_YYYY/MM/DD": "PERSIAN_YYYY/MM/DD",
    "JALALI_YYYYMMDD": "PERSIAN_YYYYMMDD",
    "JALALI_YYYYMM": "PERSIAN_YYYYMM",
    "JALALI_YYYY": "PERSIAN_YYYY",
    "JALALI_YYYY/MM/DD": "PERSIAN_YYYY/MM/DD",
    "SHAMSI_YYYYMMDD": "PERSIAN_YYYYMMDD",
    "SHAMSI_YYYYMM": "PERSIAN_YYYYMM",
    "SHAMSI_YYYY": "PERSIAN_YYYY",
    "SHAMSI_YYYY/MM/DD": "PERSIAN_YYYY/MM/DD",
}


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
            execution_date: Date in YYYYMMDD format (Gregorian / Airflow ds_nodash)
            
        Returns:
            Partition value or None if no partition column
        """
        if not self.partition_column:
            return None
        
        fmt = (self.partition_format or "").strip().upper()
        persian_fmt = _PERSIAN_ALIASES.get(fmt)

        # Gregorian formats (Airflow execution date is already Gregorian)
        if fmt == "YYYYMMDD":
            return execution_date
        if fmt == "YYYYMM":
            return execution_date[:6]
        if fmt == "YYYY":
            return execution_date[:4]

        # Persian / Jalali formats — convert Gregorian execution_date first
        if persian_fmt == "PERSIAN_YYYYMMDD":
            return to_persian_date_key(execution_date)
        if persian_fmt == "PERSIAN_YYYYMM":
            return to_persian_year_month(execution_date)
        if persian_fmt == "PERSIAN_YYYY":
            return to_persian_year(execution_date)
        if persian_fmt == "PERSIAN_YYYY/MM/DD":
            return to_persian_date(execution_date)

        raise ValueError(
            f"Unsupported partition_format={self.partition_format!r}. "
            f"Use YYYYMMDD, YYYYMM, YYYY, or "
            f"PERSIAN_YYYYMMDD, PERSIAN_YYYYMM, PERSIAN_YYYY, PERSIAN_YYYY/MM/DD "
            f"(aliases: JALALI_*, SHAMSI_*)."
        )
