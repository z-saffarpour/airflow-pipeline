"""
Orchestrator for syncing query results from MSSQL to ClickHouse (bulk INSERT).

Uses ReplacingMergeTree-friendly INSERTs via ClickHouseWriter (optional version_id).
Chunk planning uses SQL Server NTILE on the source, same as MSSQL→MySQL/MSSQL paths.
"""
import logging
import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipeline.config import MasterDataSyncConfig
from pipeline.core.TransferMetrics import TransferMetrics
from pipeline.core.TransferResult import TransferResult
from pipeline.database.ClickHouseWriter import ClickHouseWriter
from pipeline.database.MSSQLDataReader import MSSQLDataReader
from pipeline.database.SQLQueryBuilder import SQLQueryBuilder


class MSSQLToClickHouseQueryOrchestrator:
    """
    Read batches from MSSQL and bulk-insert into ClickHouse target tables.

    Notes:
    - ``target_schema`` is the ClickHouse database name.
    - Upsert semantics rely on ReplacingMergeTree (+ optional ``version_id``), not MERGE.
    - ``delete_missing``, staging, and hash-change detection are not used on this path.
    """

    def __init__(
        self,
        source_conn_id: str,
        target_conn_id: str,
        fail_on_error: bool,
        batch_size: int = 5000,
        version_id: Optional[int] = None,
    ) -> None:
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.fail_on_error = fail_on_error
        self.batch_size = batch_size
        self.version_id = version_id
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _resolve_version_id(
        version_id: Optional[int],
        execution_date: Optional[str] = None,
    ) -> Optional[int]:
        if version_id is not None:
            return version_id
        if execution_date:
            digits = re.sub(r"\D", "", str(execution_date))
            if digits:
                try:
                    return int(digits[:14].ljust(14, "0"))
                except ValueError:
                    pass
        return int(datetime.now().strftime("%Y%m%d%H%M%S"))

    @staticmethod
    def _resolve_chunk_column(sync_config: MasterDataSyncConfig) -> str:
        if sync_config.chunk_column:
            return sync_config.chunk_column
        if not sync_config.primary_keys:
            raise ValueError("chunk_column or primary_keys is required for dynamic sync tasks")
        return sync_config.primary_keys[0]

    @staticmethod
    def _normalize_source_query(source_query: str) -> str:
        return source_query.strip().rstrip(";")

    @classmethod
    def build_chunk_plan_query(
        cls,
        source_query: str,
        chunk_column: str,
        chunk_count: int,
    ) -> str:
        """Build NTILE-based chunk plan (SQL Server)."""
        SQLQueryBuilder._validate_and_raise(chunk_column, "column name")
        inner_query = cls._normalize_source_query(source_query)
        return f"""
        WITH source_data AS (
            {inner_query}
        ),
        chunked AS (
            SELECT [{chunk_column}] AS chunk_key,
                   NTILE({chunk_count}) OVER (ORDER BY [{chunk_column}]) AS chunk_no
            FROM source_data
        )
        SELECT
            chunk_no,
            MIN(chunk_key) AS min_key,
            MAX(chunk_key) AS max_key,
            COUNT(1) AS row_count
        FROM chunked
        GROUP BY chunk_no
        ORDER BY chunk_no
        """

    @classmethod
    def build_chunk_source_query(
        cls,
        source_query: str,
        chunk_column: str,
    ) -> str:
        SQLQueryBuilder._validate_and_raise(chunk_column, "column name")
        inner_query = cls._normalize_source_query(source_query)
        return f"""
        SELECT source_data.*
        FROM (
            {inner_query}
        ) AS source_data
        WHERE source_data.[{chunk_column}] >= %s
          AND source_data.[{chunk_column}] <= %s
        """

    def _create_writer(self) -> ClickHouseWriter:
        return ClickHouseWriter(conn_id=self.target_conn_id)

    def _warn_unsupported_flags(self, sync_config: MasterDataSyncConfig) -> None:
        if sync_config.delete_missing:
            self.logger.warning(
                "[MSSQLToClickHouseQueryOrchestrator] delete_missing=True is ignored; "
                "ClickHouse sync uses bulk INSERT / ReplacingMergeTree only"
            )
        if sync_config.use_hash_change_detection:
            self.logger.debug(
                "[MSSQLToClickHouseQueryOrchestrator] use_hash_change_detection is ignored "
                "on ClickHouse path"
            )

    def plan_sync_chunks(
        self,
        sync_config: MasterDataSyncConfig,
    ) -> List[Dict[str, Any]]:
        """Build key ranges for parallel dynamic sync tasks (SQL Server NTILE)."""
        chunk_column = self._resolve_chunk_column(sync_config)
        reader = MSSQLDataReader(
            conn_id=self.source_conn_id,
            batch_size=sync_config.batch_size,
        )

        total_count = reader.get_total_count(
            custom_query=sync_config.source_query_count,
            params=None,
        )
        if total_count == 0:
            self.logger.info(
                "[MSSQLToClickHouseQueryOrchestrator.plan_sync_chunks] No rows to plan | "
                "table=%s",
                sync_config.target_table,
            )
            return []

        chunk_count = max(1, math.ceil(total_count / sync_config.task_chunk_size))
        plan_query = self.build_chunk_plan_query(
            sync_config.source_query,
            chunk_column,
            chunk_count,
        )
        rows = reader.connection_factory.execute_query(plan_query)

        chunks: List[Dict[str, Any]] = []
        for row in rows:
            chunk_no = row.get("chunk_no") or row.get("CHUNK_NO")
            min_key = row.get("min_key") if "min_key" in row else row.get("MIN_KEY")
            max_key = row.get("max_key") if "max_key" in row else row.get("MAX_KEY")
            row_count = row.get("row_count") if "row_count" in row else row.get("ROW_COUNT")
            chunks.append(
                {
                    "chunk_no": int(chunk_no),
                    "min_key": min_key,
                    "max_key": max_key,
                    "row_count": int(row_count or 0),
                    "chunk_column": chunk_column,
                }
            )

        self.logger.info(
            "[MSSQLToClickHouseQueryOrchestrator.plan_sync_chunks] Planned %d chunks | "
            "table=%s | total_rows=%s",
            len(chunks),
            sync_config.target_table,
            total_count,
        )
        return chunks

    def sync_data_chunk(
        self,
        sync_config: MasterDataSyncConfig,
        chunk: Dict[str, Any],
        execution_date: str,
    ) -> TransferResult:
        """Sync one key-range chunk from MSSQL to ClickHouse."""
        chunk_no = chunk["chunk_no"]
        if chunk.get("skip"):
            return TransferResult.create_success(
                records_transferred=0,
                batch_count=0,
                duration_seconds=0,
                total_records=0,
                inserted=0,
                updated=0,
                deleted=0,
            )

        chunk_column = chunk.get("chunk_column") or self._resolve_chunk_column(sync_config)
        min_key = chunk["min_key"]
        max_key = chunk["max_key"]
        expected_rows = chunk.get("row_count", 0)
        version_id = self._resolve_version_id(self.version_id, execution_date)

        metrics = TransferMetrics(
            table_name=f"{sync_config.source_name}_chunk_{chunk_no}",
            execution_date=execution_date,
        )
        metrics.total_records = expected_rows

        chunk_query = self.build_chunk_source_query(
            sync_config.source_query,
            chunk_column,
        )
        query_params = (min_key, max_key)

        self.logger.info(
            "[MSSQLToClickHouseQueryOrchestrator.sync_data_chunk] Starting | table=%s | "
            "chunk=%s | %s=[%s, %s] | expected_rows=%s",
            sync_config.target_table,
            chunk_no,
            chunk_column,
            min_key,
            max_key,
            expected_rows,
        )

        try:
            self._warn_unsupported_flags(sync_config)
            reader = MSSQLDataReader(
                conn_id=self.source_conn_id,
                batch_size=self.batch_size,
            )
            writer = self._create_writer()

            batch_number = 0
            for batch in reader.stream_query(
                query=chunk_query,
                count_query=None,
                parameters=query_params,
            ):
                batch_number += 1
                batch_size = len(batch)

                inserted = writer.upsert_batch(
                    database=sync_config.target_schema,
                    table_name=sync_config.target_table,
                    batch=batch,
                    version_id=version_id,
                )

                metrics.inserted += inserted
                metrics.increment_batch(batch_size)

            metrics.mark_completed()
            return TransferResult.create_success(
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=expected_rows or metrics.transferred_records,
                inserted=metrics.inserted,
                updated=0,
                deleted=0,
            )
        except Exception as exc:
            error_message = (
                f"Chunk sync failed for {sync_config.source_name} "
                f"chunk={chunk_no}: {exc}"
            )
            metrics.mark_failed(error_message)
            return TransferResult.create_failure(
                error_message=error_message,
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=metrics.total_records,
                inserted=metrics.inserted,
                updated=0,
                deleted=0,
            )

    def sync_data(
        self,
        sync_config: MasterDataSyncConfig,
        execution_date: str,
    ) -> TransferResult:
        metrics = TransferMetrics(
            table_name=sync_config.source_name,
            execution_date=execution_date,
        )
        total_count = None
        version_id = self._resolve_version_id(self.version_id, execution_date)

        self.logger.info(
            "[MSSQLToClickHouseQueryOrchestrator.sync_data] Starting MSSQL → ClickHouse "
            "sync | source=%s | target=%s.%s | batch_size=%s | version_id=%s",
            self.source_conn_id,
            sync_config.target_schema,
            sync_config.target_table,
            self.batch_size,
            version_id,
        )

        try:
            self._warn_unsupported_flags(sync_config)
            reader = MSSQLDataReader(
                conn_id=self.source_conn_id,
                batch_size=self.batch_size,
            )

            if sync_config.source_query_count is not None:
                total_count = reader.get_total_count(
                    custom_query=sync_config.source_query_count,
                    params=None,
                )
                metrics.total_records = total_count

                if total_count == 0:
                    metrics.mark_completed()
                    return TransferResult.create_success(
                        records_transferred=0,
                        batch_count=0,
                        duration_seconds=metrics.duration_seconds,
                        total_records=total_count,
                        inserted=0,
                        updated=0,
                        deleted=0,
                    )

            writer = self._create_writer()

            batch_number = 0
            for batch in reader.stream_query(
                query=sync_config.source_query,
                count_query=sync_config.source_query_count,
                parameters=None,
            ):
                batch_number += 1
                batch_size = len(batch)

                inserted = writer.upsert_batch(
                    database=sync_config.target_schema,
                    table_name=sync_config.target_table,
                    batch=batch,
                    version_id=version_id,
                )

                metrics.inserted += inserted
                metrics.increment_batch(batch_size)

                self.logger.info(
                    "[MSSQLToClickHouseQueryOrchestrator.sync_data] Batch %s | rows=%s | "
                    "transferred=%s",
                    batch_number,
                    batch_size,
                    metrics.transferred_records,
                )

            metrics.mark_completed()
            self.logger.info(
                "[MSSQLToClickHouseQueryOrchestrator.sync_data] Completed | table=%s | "
                "transferred=%s | inserted=%s | duration=%ss",
                sync_config.source_name,
                metrics.transferred_records,
                metrics.inserted,
                metrics.duration_seconds,
            )

            return TransferResult.create_success(
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=total_count if sync_config.source_query_count is not None else None,
                inserted=metrics.inserted,
                updated=0,
                deleted=0,
            )

        except Exception as exc:
            error_message = f"Transfer failed for {sync_config.source_name}: {exc}"
            self.logger.error(
                "[MSSQLToClickHouseQueryOrchestrator.sync_data] Failed | table=%s | error=%s",
                sync_config.source_name,
                exc,
                exc_info=True,
            )
            metrics.mark_failed(error_message)
            return TransferResult.create_failure(
                error_message=error_message,
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=metrics.total_records,
                inserted=metrics.inserted,
                updated=0,
                deleted=0,
            )
