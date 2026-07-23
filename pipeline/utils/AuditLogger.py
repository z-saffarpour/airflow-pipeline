# pipeline/utils/audit.py
"""
Audit logging utility for tracking pipeline events.
Logs are written in JSONL format for easy parsing and analysis.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from pipeline.config.AuditConfig import AUDIT_CONFIG, EventStatus

class AuditLogger:
    """
    Centralized audit logger for pipeline events.
    
    Usage:
        audit = AuditLogger(dag_id='my_dag', run_id='manual__2026-03-28')
        audit.log('kafka_produce', 'task_1', 'success', {'count': 100})
    """
    
    def __init__(
        self, 
        dag_id: str, 
        run_id: str,
        log_dir: str = None
    ):
        self.dag_id = dag_id
        self.run_id = run_id
        self.log_dir = Path(log_dir or AUDIT_CONFIG['log_dir'])
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        logger_name = f"audit.{dag_id}" #.{self.run_id}
        self.logger = logging.getLogger(logger_name)
        if not self.logger.handlers:
            self._setup_logger()
    
    def _setup_logger(self):
        """Setup JSON logger with file handler."""
        if not self.logger.handlers:
            self.logger.setLevel(logging.INFO)        
            log_file = self.log_dir / f"{self.dag_id}_{self.run_id}.jsonl"
            handler = logging.FileHandler(log_file)
            handler.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(handler)
    
    def log(
        self,
        event_type: str,
        task_id: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> None:
        """
        Log an audit event.
        
        Args:
            event_type: Event category (e.g., 'kafka_produce', 'chunk_processing')
            task_id: Airflow task identifier
            status: Event status ('started', 'success', 'failed', 'retry', 'warning')
            details: Additional event data
            error: Error message if status is 'failed'
        """
        entry = {
            'timestamp': datetime.now().isoformat(),
            'dag_id': self.dag_id,
            'run_id': self.run_id,
            'task_id': task_id,
            'event_type': event_type,
            'status': status,
            'details': details or {}
        }
    
        if error:
            entry["error"] = error
        
        log_message = json.dumps(entry, ensure_ascii=False)
        
        if status == EventStatus.FAILED:
            self.logger.error(log_message)
        elif status == EventStatus.WARNING:
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)