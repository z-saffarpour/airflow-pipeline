"""
Transfer Result dataclass for encapsulating transfer operation results.
Provides structured result information for better error handling and reporting.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class TransferResult:
    """
    Result of a data transfer operation.
    Encapsulates success/failure status and relevant metrics.
    """
    
    success: bool
    records_transferred: int
    batch_count: int
    duration_seconds: float
    error_message: Optional[str] = None
    total_records: Optional[int] = None
    inserted: Optional[int] = None
    updated: Optional[int] = None
    deleted: Optional[int] = None
    
    @property
    def is_success(self) -> bool:
        """Check if transfer was successful."""
        return self.success
    
    @property
    def is_failure(self) -> bool:
        """Check if transfer failed."""
        return not self.success
    
    def get_summary(self) -> str:
        """
        Get a summary message of the transfer result.
        
        Returns:
            Human-readable summary string
        """
        if self.success:
            records_info = f"{self.records_transferred:,} records"
            if self.total_records:
                records_info += f" of {self.total_records:,}"
            
            return (
                f"Transfer successful: {records_info} "
                f"in {self.duration_seconds:.2f}s "
                f"({self.batch_count} batches)"
            )
        else:
            return (
                f"Transfer failed: {self.error_message}. "
                f"Transferred {self.records_transferred:,} records before failure."
            )
    
    def to_dict(self) -> dict:
        """
        Convert result to dictionary.
        
        Returns:
            Dictionary representation of the result
        """
        return {
            'success': self.success,
            'records_transferred': self.records_transferred,
            'batch_count': self.batch_count,
            'duration_seconds': self.duration_seconds,
            'error_message': self.error_message,
            'total_records': self.total_records,
            'inserted': self.inserted,
            'updated': self.updated,
            'deleted': self.deleted,
        }
    
    @classmethod
    def create_success(
        cls,
        records_transferred: int,
        batch_count: int,
        duration_seconds: float,
        total_records: Optional[int] = None,
        inserted: Optional[int] = None,
        updated: Optional[int] = None,
        deleted: Optional[int] = None,
    ) -> 'TransferResult':
        """
        Create a successful transfer result.
        
        Args:
            records_transferred: Number of records transferred
            batch_count: Number of batches processed
            duration_seconds: Total duration in seconds
            total_records: Total number of records (optional)
        
        Returns:
            TransferResult instance
        """
        return cls(
            success=True,
            records_transferred=records_transferred,
            batch_count=batch_count,
            duration_seconds=duration_seconds,
            total_records=total_records,
            inserted=inserted,
            updated=updated,
            deleted=deleted,
        )
    
    @classmethod
    def create_failure(
        cls,
        error_message: str,
        records_transferred: int = 0,
        batch_count: int = 0,
        duration_seconds: float = 0.0,
        total_records: Optional[int] = None,
        inserted: Optional[int] = None,
        updated: Optional[int] = None,
        deleted: Optional[int] = None,
    ) -> 'TransferResult':
        """
        Create a failed transfer result.
        
        Args:
            error_message: Error description
            records_transferred: Number of records transferred before failure
            batch_count: Number of batches processed before failure
            duration_seconds: Duration before failure
            total_records: Total number of records (optional)
        
        Returns:
            TransferResult instance
        """
        return cls(
            success=False,
            records_transferred=records_transferred,
            batch_count=batch_count,
            duration_seconds=duration_seconds,
            error_message=error_message,
            total_records=total_records,
            inserted=inserted,
            updated=updated,
            deleted=deleted,
        )
