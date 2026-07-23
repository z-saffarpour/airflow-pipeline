"""
Transfer Metrics for tracking and reporting data transfer statistics.
Provides observability into the ETL process.
"""
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime


@dataclass
class TransferMetrics:
    """
    Tracks metrics for a data transfer operation.
    Provides observability and monitoring capabilities.
    """
    
    # Transfer identification
    table_name: str
    execution_date: str
    
    # Timing metrics
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    
    # Transfer metrics
    total_records: int = 0
    transferred_records: int = 0
    failed_records: int = 0
    batch_count: int = 0
    
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    
    # Performance metrics
    records_per_second: float = 0.0
    duration_seconds: float = 0.0
    
    # Status
    status: str = "in_progress"  # in_progress, completed, failed
    error_message: Optional[str] = None

    def mark_completed(self) -> None:
        """Mark the transfer as completed and calculate final metrics."""
        self.end_time = datetime.now()
        self.status = "completed"
        self._calculate_duration_and_rate()

    def mark_failed(self, error_message: str) -> None:
        """
        Mark the transfer as failed.

        Args:
            error_message: Description of the failure
        """
        self.end_time = datetime.now()
        self.status = "failed"
        self.error_message = error_message
        self._calculate_duration_and_rate()

    def increment_batch(self, records_in_batch: int) -> None:
        """
        Increment batch counter and record count.

        Args:
            records_in_batch: Number of records in the current batch
        """
        self.batch_count += 1
        self.transferred_records += records_in_batch

    def increment_failed(self, count: int = 1) -> None:
        """
        Increment failed record counter.

        Args:
            count: Number of failed records to add
        """
        self.failed_records += count

    def _calculate_duration_and_rate(self) -> None:
        """Calculate duration and transfer rate."""
        if self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
            self.duration_seconds = duration
            if duration > 0:
                self.records_per_second = self.transferred_records / duration

    def get_progress_percentage(self) -> float:
        """
        Calculate transfer progress percentage.

        Returns:
            Progress as percentage (0-100)
        """
        if self.total_records == 0:
            return 0.0
        return (self.transferred_records / self.total_records) * 100

    def to_dict(self) -> dict:
        """
        Convert metrics to dictionary for logging or storage.

        Returns:
            Dictionary representation of metrics
        """
        return {
            'table_name': self.table_name,
            'execution_date': self.execution_date,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'total_records': self.total_records,
            'transferred_records': self.transferred_records,
            'failed_records': self.failed_records,
            'batch_count': self.batch_count,
            'records_per_second': self.records_per_second,
            'duration_seconds': self.duration_seconds,
            'status': self.status,
            'error_message': self.error_message,
            'progress_percentage': self.get_progress_percentage(),
        }

    def get_summary_message(self) -> str:
        """
        Get a human-readable summary of the transfer.

        Returns:
            Summary string
        """
        if self.status == "completed":
            return (
                f"Transfer completed successfully: {self.transferred_records:,} records "
                f"from {self.table_name} in {self.duration_seconds:.2f}s "
                f"({self.records_per_second:.2f} records/sec, {self.batch_count} batches)"
            )
        elif self.status == "failed":
            return (
                f"Transfer failed: {self.transferred_records:,}/{self.total_records:,} records "
                f"transferred before failure. Error: {self.error_message}"
            )
        else:
            return (
                f"Transfer in progress: {self.transferred_records:,}/{self.total_records:,} records "
                f"({self.get_progress_percentage():.1f}%)"
            )
