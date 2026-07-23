"""
Abstract Base Class for Data Writer.
Provides contract for implementing different data sinks.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Tuple

# Row alias for readability
Row = Dict[str, Any]
UniqueKeyColumns = Tuple[str, ...]
UniqueKeys = Tuple[UniqueKeyColumns, ...]


class DataWriter(ABC):
    """
    Abstract interface for writing data to various sinks.
    Implement this interface to add support for different databases.
    """

    @abstractmethod
    def insert_batch(
        self,
        schema: str,
        table: str,
        data: List[Row],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        """
        Insert data in batches.

        Args:
            schema: Target schema name
            table: Target table name
            data: List of Row containing row data
            batch_size: Number of records per batch
            staging_schema: Schema for temporary staging tables; defaults to schema when omitted

        Returns:
            Number of records inserted
        """
        raise NotImplementedError()

    @abstractmethod
    def update_batch(
        self,
        schema: str,
        table: str,
        data: List[Row],
        key_columns: List[str],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        """
        Update records in batches based on key columns.

        Args:
            schema: Target schema name
            table: Target table name
            data: List of Row containing row data
            key_columns: List of column names that form the primary key
            batch_size: Number of records per batch
            staging_schema: Schema for temporary staging tables; defaults to schema when omitted

        Returns:
            Number of records updated
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_batch(
        self,
        schema: str,
        table: str,
        key_column: str,
        key_values: List[Any],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        """
        Delete records in batches based on key values.

        Args:
            schema: Target schema name
            table: Target table name
            key_column: Column name for deletion
            key_values: List of key values to delete
            batch_size: Number of records per batch
            staging_schema: Schema for temporary staging tables; defaults to schema when omitted

        Returns:
            Number of records deleted
        """
        raise NotImplementedError()

    @abstractmethod
    def upsert_batch(
        self,
        schema: str,
        table: str,
        data: List[Row],
        key_columns: List[str],
        batch_size: int = 1000,
        delete_missing: bool = False,
        unique_keys: Optional[UniqueKeys] = None,
        resolve_unique_key_conflicts: bool = False,
        use_hash_change_detection: bool = True,
        staging_schema: Optional[str] = None,
        staging_suffix: str = "upsert",
    ) -> Dict[str, int]:
        """
        Perform UPSERT (insert or update) operation in batches.

        Args:
            schema: Target schema name
            table: Target table name
            data: List of Row containing row data
            key_columns: List of column names that form the primary key
            batch_size: Number of records per batch
            delete_missing: Whether to delete records not in source
            unique_keys: Non-primary unique key column sets for conflict resolution
            resolve_unique_key_conflicts: Discover and resolve unique key conflicts before MERGE
            use_hash_change_detection: Compare row versions with hash instead of per-column checks
            staging_schema: Schema for temporary staging tables; defaults to schema when omitted
            staging_suffix: Unique suffix for the staging table name; use per-chunk values for parallel syncs

        Returns:
            Dictionary with operation results (inserted, updated, deleted, ...)
        """
        raise NotImplementedError()
