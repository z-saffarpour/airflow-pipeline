"""
Abstract Base Class for Sync Orchestrator.
Provides a common contract for source-to-target data synchronization.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List

if TYPE_CHECKING:
    # Deferred: importing pipeline.core.TransferResult for real would pull in
    # pipeline.core.__init__, which imports every orchestrator - including
    # concrete implementations of this interface. TYPE_CHECKING + `from
    # __future__ import annotations` keep this hint available to type
    # checkers/IDEs without creating that import cycle at runtime.
    from pipeline.core.TransferResult import TransferResult


class SyncOrchestrator(ABC):
    """
    Abstract interface for orchestrating a source-to-target data sync.

    Implemented by the "*To*QueryOrchestrator" classes in pipeline.core
    (MSSQLToMSSQLQueryOrchestrator, MongoDBToMSSQLQueryOrchestrator,
    KafkaToMSSQLQueryOrchestrator, ClickHouseToMSSQLQueryOrchestrator, and
    their siblings). Each orchestrator is constructed with source/target
    connection info and coordinates a reader and writer to move data in
    batches, with optional dynamic chunking for parallel Airflow tasks.

    ``sync_config`` is intentionally typed as ``Any``: each orchestrator
    accepts its own dataclass (MasterDataSyncConfig, MongoSyncConfig,
    KafkaSyncConfig, MSSQLToKafkaSyncConfig, ...), which does not currently
    share one common base across all of them.

    Not every orchestrator in pipeline.core fits this shape -
    ClickHouseOptimizationOrchestrator (table maintenance, no source/target
    sync) and MSSQLDataTransferOrchestrator (a different, non-chunked
    transfer flow) intentionally do not implement this interface.
    """

    @abstractmethod
    def sync_data(self, sync_config: Any, execution_date: str) -> "TransferResult":
        """
        Run a full, non-chunked sync from source to target.

        Args:
            sync_config: Orchestrator-specific sync configuration
            execution_date: Airflow execution date (e.g. YYYYMMDD)

        Returns:
            TransferResult summarizing the sync outcome
        """
        pass

    @abstractmethod
    def plan_sync_chunks(self, sync_config: Any) -> List[Dict[str, Any]]:
        """
        Plan chunks (key ranges, partitions, buckets, ...) for parallel,
        dynamic-task-mapped sync runs.

        Args:
            sync_config: Orchestrator-specific sync configuration

        Returns:
            List of chunk descriptor dicts, each consumable by
            sync_data_chunk
        """
        pass

    @abstractmethod
    def sync_data_chunk(
        self,
        sync_config: Any,
        chunk: Dict[str, Any],
        execution_date: str,
    ) -> "TransferResult":
        """
        Sync a single chunk previously produced by plan_sync_chunks.

        Args:
            sync_config: Orchestrator-specific sync configuration
            chunk: One chunk descriptor from plan_sync_chunks
            execution_date: Airflow execution date

        Returns:
            TransferResult summarizing that chunk's sync outcome
        """
        pass
