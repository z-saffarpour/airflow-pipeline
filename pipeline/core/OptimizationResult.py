"""
Optimization Result - Immutable result object for optimization operations.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class OptimizationResult:
    """Immutable result of an optimization operation."""
    
    table_name: str
    execution_date: str
    status: str  # 'success', 'failed', 'skipped'
    duration_seconds: float = 0.0
    partition: Optional[str] = None
    error_message: Optional[str] = None
    parts_before: int = 0
    parts_after: int = 0
    rows_optimized: int = 0
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    @property
    def is_success(self) -> bool:
        """Check if optimization succeeded."""
        return self.status == 'success'
    
    @property
    def is_failure(self) -> bool:
        """Check if optimization failed."""
        return self.status == 'failed'
    
    @property
    def parts_merged(self) -> int:
        """Calculate number of parts merged."""
        return max(0, self.parts_before - self.parts_after)
    
    def get_summary(self) -> str:
        """Get human-readable summary of result."""
        if self.is_success:
            return (
                f"Optimization SUCCESS: {self.table_name} "
                f"(partition={self.partition or 'all'}) "
                f"- {self.parts_merged} parts merged in {self.duration_seconds:.2f}s"
            )
        elif self.is_failure:
            return (
                f"Optimization FAILED: {self.table_name} "
                f"Error: {self.error_message}"
            )
        else:
            return f"○ Optimization SKIPPED: {self.table_name}"
