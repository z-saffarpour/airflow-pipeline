"""
Author: Senior Data Engineer
Version: 1.0
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional, List
from typing import ClassVar
import re

@dataclass
class DAGConfig:
    # Excluded from __init__ and dataclass processing
    _DAG_ID_PATTERN: ClassVar[re.Pattern] = re.compile(r"^[a-zA-Z0-9_\-\.]+$")
            
    # Required    
    dag_id: Optional[str] = None
    description: Optional[str] = None
    
    # Date config
    date_offset: Optional[int] = None
    
    # Core Airflow config
    schedule: Optional[str] = None
    start_date: datetime = field(default_factory=lambda: datetime(2026, 3, 1))
    catchup: bool = False
    
    # Retry & execution
    retries: int = 2
    retry_delay: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    sync_retries: int = 3
    sync_retry_delay: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    execution_timeout: Optional[timedelta] = field(default_factory=lambda: timedelta(hours=8))
    max_active_runs: int = 1
    max_active_tasks: int = 10

    is_paused_upon_creation: bool = True

    # Execution Control
    pool: str = "data_sync_pool"
    #priority_weight: int = 1
    depends_on_past: bool = False
    
    # Metadata
    tags: List[str] = field(default_factory=list)
    owner: str = "data-engineering"
    
    def __post_init__(self):
        # --- dag_id validation ---
        if not self.dag_id:
            raise ValueError(
                f"[{self.__class__.__name__}] 'dag_id' is required and cannot be None or empty."
            )
        if not self._DAG_ID_PATTERN.match(self.dag_id):
            raise ValueError(
                f"[{self.__class__.__name__}] Invalid dag_id='{self.dag_id}'. "
                "Only alphanumeric, underscore, dash, and dot are allowed."
            )
                    
        # --- description validation ---
        if not self.description:
            raise ValueError(
                f"[{self.__class__.__name__}] 'description' is required."
            )

        # --- numeric validations ---
        if self.retries < 0:
            raise ValueError("'retries' must be >= 0")
        if self.sync_retries < 0:
            raise ValueError("'sync_retries' must be >= 0")
        if self.max_active_runs < 1:
            raise ValueError("'max_active_runs' must be >= 1")