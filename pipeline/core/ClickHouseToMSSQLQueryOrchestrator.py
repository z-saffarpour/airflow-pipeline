"""
Orchestrator for syncing query results from ClickHouse to MSSQL (upsert).
"""
import logging
import math
import re
from typing import Any, Dict, List, Optional, Tuple

from pipeline.config import MasterDataSyncConfig
from pipeline.core.TransferMetrics import TransferMetrics
from pipeline.core.TransferResult import TransferResult
from pipeline.database.ClickHouseDataReader import ClickHouseDataReader
from pipeline.database.MSSQLServerWriter import MSSQLServerWriter
from pipeline.utils.IdentifierValidator import IdentifierValidator
from pipeline.interfaces.SyncOrchestrator import SyncOrchestrator


class ClickHouseToMSSQLQueryOrchestrator(SyncOrchestrator):
    """
    Read batches from ClickHouse and upsert into MSSQL target tables.
    Mirrors MySQLToMSSQLQueryOrchestrator behaviour with ClickHouse source dialect.
    """

    def __init__(
        self,
        source_conn_id: str,
        target_conn_id: str,
        fail_on_error: bool,
        batch_size: int = 5000,
        target_is_connection_string: bool = False,
    ) -> None:
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.target_is_connection_string = target_is_connection_string
        self.fail_on_error = fail_on_error
        self.batch_size = batch_size
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _resolve_chunk_column(sync_config: MasterDataSyncConfig) -> str:
        if sync_config.chunk_column:
            return sync_config.chunk_column
        if not sync_config.primary_keys:
            raise ValueError(
                "chunk_column or primary_keys is required for dynamic sync tasks"
            )
        return sync_config.primary_keys[0]

    @staticmethod
    def _resolve_delete_scope_column(sync_config: MasterDataSyncConfig) -> str:
        if sync_config.delete_scope_column:
            return sync_config.delete_scope_column
        if sync_config.chunk_column:
            return sync_config.chunk_column
        raise ValueError(
            "delete_scope_column or chunk_column is required when delete_missing=True"
        )

    @staticmethod
    def _resolve_staging_schema(sync_config: MasterDataSyncConfig) -> Optional[str]:
        schema = sync_config.staging_schema
        if schema:
            IdentifierValidator.validate_and_raise(schema, "staging schema")
        return schema

    @staticmethod
    def _normalize_source_query(source_query: str) -> str:
        return source_query.strip().rstrip(";")

    @staticmethod
    def _quote_ident(identifier: str) -> str:
        IdentifierValidator.validate_and_raise(identifier, "column name")
        return f"`{identifier}`"

    @classmethod
    def build_chunk_plan_query(
        cls,
        source_query: str,
        chunk_column: str,
        chunk_count: int,
    ) -> str:
        """Build ntile-based chunk plan (ClickHouse window functions)."""
        quoted = cls._quote_ident(chunk_column)
        inner_query = cls._normalize_source_query(source_query)
        return f"""
        WITH source_data AS (
            {inner_query}
        ),
        chunked AS (
            SELECT {quoted} AS chunk_key,
                   ntile({chunk_count}) OVER (ORDER BY {quoted}) AS chunk_no
            FROM source_data
        )
        SELECT
            chunk_no,
            min(chunk_key) AS min_key,
            max(chunk_key) AS max_key,
            count() AS row_count
        FROM chunked
        GROUP BY chunk_no
        ORDER BY chunk_no
        """

    @classmethod
    def build_scope_bounds_query(
        cls,
        source_query: str,
        chunk_column: str,
    ) -> str:
        quoted = cls._quote_ident(chunk_column)
        inner_query = cls._normalize_source_query(source_query)
        return f"""
        SELECT
            min({quoted}) AS min_key,
            max({quoted}) AS max_key
        FROM (
            {inner_query}
        ) AS source_data
        """

    @classmethod
    def build_chunk_delete_scope_bounds_query(
        cls,
        source_query: str,
        chunk_column: str,
        delete_scope_column: str,
    ) -> str:
        quoted_chunk = cls._quote_ident(chunk_column)
        quoted_scope = cls._quote_ident(delete_scope_column)
        inner_query = cls._normalize_source_query(source_query)
        return f"""
        SELECT
            min({quoted_scope}) AS min_key,
            max({quoted_scope}) AS max_key
        FROM (
            {inner_query}
        ) AS source_data
        WHERE source_data.{quoted_chunk} >= %(min_key)s
          AND source_data.{quoted_chunk} <= %(max_key)s
        """

    @classmethod
    def build_chunk_source_query(
        cls,
        source_query: str,
        chunk_column: str,
    ) -> str:
        quoted = cls._quote_ident(chunk_column)
        inner_query = cls._normalize_source_query(source_query)
        return f"""
        SELECT source_data.*
        FROM (
            {inner_query}
        ) AS source_data
        WHERE source_data.{quoted} >= %(min_key)s
          AND source_data.{quoted} <= %(max_key)s
        """

    @staticmethod
    def _sanitize_staging_suffix(value: str) -> str:
        return re.sub(r"[^\w]", "_", str(value))[:50]

    def _create_writer(self) -> MSSQLServerWriter:
        return MSSQLServerWriter(
            conn_id=self.target_conn_id,
            is_connection_string=self.target_is_connection_string,
        )

    def _fetch_scope_bounds(
        self,
        reader: ClickHouseDataReader,
        source_query: str,
        chunk_column: str,
    ) -> Tuple[Any, Any]:
        bounds_query = self.build_scope_bounds_query(source_query, chunk_column)
        rows = reader.connection_factory.execute_query_as_dicts(bounds_query)
        if not rows:
            return None, None
        row = rows[0]
        min_key = row.get("min_key") if "min_key" in row else row.get("MIN_KEY")
        max_key = row.get("max_key") if "max_key" in row else row.get("MAX_KEY")
        return min_key, max_key

    def _fetch_chunk_delete_scope_bounds(
        self,
        reader: ClickHouseDataReader,
        source_query: str,
        chunk_column: str,
        min_key: Any,
        max_key: Any,
        delete_scope_column: str,
    ) -> Tuple[Any, Any]:
        bounds_query = self.build_chunk_delete_scope_bounds_query(
            source_query,
            chunk_column,
            delete_scope_column,
        )
        rows = reader.connection_factory.execute_query_as_dicts(
            bounds_query,
            {"min_key": min_key, "max_key": max_key},
        )
        if not rows:
            return None, None
        row = rows[0]
        scope_min = row.get("min_key") if "min_key" in row else row.get("MIN_KEY")
        scope_max = row.get("max_key") if "max_key" in row else row.get("MAX_KEY")
        return scope_min, scope_max

    def _finalize_delete_missing(
        self,
        writer: MSSQLServerWriter,
        sync_config: MasterDataSyncConfig,
        keys_staging_table: str,
        scope_column: str,
        min_key: Any,
        max_key: Any,
    ) -> int:
        try:
            if min_key is None or max_key is None:
                self.logger.info(
                    f"[ClickHouseToMSSQLQueryOrchestrator._finalize_delete_missing] Skipping scoped delete | "
                    f"table={sync_config.target_table} | reason=empty source scope bounds"
                )
                return 0

            deleted = writer.delete_missing_in_scope(
                schema=sync_config.target_schema,
                table=sync_config.target_table,
                key_columns=list(sync_config.primary_keys),
                keys_staging_table=keys_staging_table,
                scope_column=scope_column,
                min_key=min_key,
                max_key=max_key,
            )
            self.logger.info(
                f"[ClickHouseToMSSQLQueryOrchestrator._finalize_delete_missing] Scoped delete completed | "
                f"table={sync_config.target_table} | deleted={deleted}"
            )
            return deleted
        finally:
            writer.drop_keys_staging_table(keys_staging_table)

    def plan_sync_chunks(
        self,
        sync_config: MasterDataSyncConfig,
    ) -> List[Dict[str, Any]]:
        """Build key ranges for parallel dynamic sync tasks (ClickHouse ntile)."""
        chunk_column = self._resolve_chunk_column(sync_config)
        reader = ClickHouseDataReader(
            conn_id=self.source_conn_id,
            batch_size=sync_config.batch_size,
        )

        total_count = reader.get_total_count(
            custom_query=sync_config.source_query_count,
            params=None,
        )
        if total_count == 0:
            self.logger.info(
                f"[ClickHouseToMSSQLQueryOrchestrator.plan_sync_chunks] No rows to plan | table={sync_config.target_table}"
            )
            return []

        chunk_count = max(1, math.ceil(total_count / sync_config.task_chunk_size))
        plan_query = self.build_chunk_plan_query(
            sync_config.source_query,
            chunk_column,
            chunk_count,
        )
        rows = reader.connection_factory.execute_query_as_dicts(plan_query)

        chunks: List[Dict[str, Any]] = []
        for row in rows:
            chunk_no = row.get("chunk_no") or row.get("CHUNK_NO")
            min_key = row.get("min_key") if "min_key" in row else row.get("MIN_KEY")
            max_key = row.get("max_key") if "max_key" in row else row.get("MAX_KEY")
            row_count = (
                row.get("row_count") if "row_count" in row else row.get("ROW_COUNT")
            )
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
            f"[ClickHouseToMSSQLQueryOrchestrator.plan_sync_chunks] Planned {len(chunks)} chunks | "
            f"table={sync_config.target_table} | total_rows={total_count}"
        )
        return chunks

    def sync_data_chunk(
        self,
        sync_config: MasterDataSyncConfig,
        chunk: Dict[str, Any],
        execution_date: str,
    ) -> TransferResult:
        """Sync one key-range chunk from ClickHouse to MSSQL."""
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

        chunk_column = chunk.get("chunk_column") or self._resolve_chunk_column(
            sync_config
        )
        min_key = chunk["min_key"]
        max_key = chunk["max_key"]
        expected_rows = chunk.get("row_count", 0)

        metrics = TransferMetrics(
            table_name=f"{sync_config.source_name}_chunk_{chunk_no}",
            execution_date=execution_date,
        )
        metrics.total_records = expected_rows

        chunk_query = self.build_chunk_source_query(
            sync_config.source_query,
            chunk_column,
        )
        query_params = {"min_key": min_key, "max_key": max_key}

        self.logger.info(
            f"[ClickHouseToMSSQLQueryOrchestrator.sync_data_chunk] Starting | table={sync_config.target_table} | "
            f"chunk={chunk_no} | {chunk_column}=[{min_key}, {max_key}] | expected_rows={expected_rows}"
        )

        try:
            reader = ClickHouseDataReader(
                conn_id=self.source_conn_id,
                batch_size=self.batch_size,
            )
            writer = self._create_writer()
            staging_schema = self._resolve_staging_schema(sync_config)
            primary_keys = list(sync_config.primary_keys)
            keys_staging_table = None

            if sync_config.delete_missing:
                keys_staging_table = writer.prepare_keys_staging_table(
                    schema=sync_config.target_schema,
                    table=sync_config.target_table,
                    key_columns=primary_keys,
                    suffix=f"chunk_{chunk_no}",
                    staging_schema=staging_schema,
                )

            batch_number = 0
            for batch in reader.stream_query(
                query=chunk_query,
                count_query=None,
                parameters=query_params,
            ):
                batch_number += 1
                batch_size = len(batch)

                batch_result = writer.upsert_batch(
                    schema=sync_config.target_schema,
                    table=sync_config.target_table,
                    data=batch,
                    key_columns=primary_keys,
                    batch_size=self.batch_size,
                    delete_missing=False,
                    unique_keys=sync_config.unique_keys,
                    resolve_unique_key_conflicts=sync_config.resolve_unique_key_conflicts,
                    use_hash_change_detection=sync_config.use_hash_change_detection,
                    staging_schema=staging_schema,
                    staging_suffix=f"upsert_{chunk_no}",
                ) or {}

                if sync_config.delete_missing and keys_staging_table and batch:
                    writer.append_keys_to_staging(
                        keys_staging_table=keys_staging_table,
                        data=batch,
                        key_columns=primary_keys,
                        batch_size=self.batch_size,
                    )

                metrics.inserted += batch_result.get("inserted", 0)
                metrics.updated += batch_result.get("updated", 0)
                metrics.deleted += batch_result.get("deleted", 0)
                metrics.increment_batch(batch_size)

            if sync_config.delete_missing and keys_staging_table:
                delete_scope_column = self._resolve_delete_scope_column(sync_config)
                if delete_scope_column == chunk_column:
                    delete_min_key, delete_max_key = min_key, max_key
                else:
                    delete_min_key, delete_max_key = (
                        self._fetch_chunk_delete_scope_bounds(
                            reader,
                            sync_config.source_query,
                            chunk_column,
                            min_key,
                            max_key,
                            delete_scope_column,
                        )
                    )
                scoped_deleted = self._finalize_delete_missing(
                    writer=writer,
                    sync_config=sync_config,
                    keys_staging_table=keys_staging_table,
                    scope_column=delete_scope_column,
                    min_key=delete_min_key,
                    max_key=delete_max_key,
                )
                metrics.deleted += scoped_deleted

            metrics.mark_completed()
            return TransferResult.create_success(
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=expected_rows or metrics.transferred_records,
                inserted=metrics.inserted,
                updated=metrics.updated,
                deleted=metrics.deleted,
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
                updated=metrics.updated,
                deleted=metrics.deleted,
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

        self.logger.info(
            f"[ClickHouseToMSSQLQueryOrchestrator.sync_data] Starting ClickHouse → MSSQL sync | "
            f"source={self.source_conn_id} | target={sync_config.target_schema}.{sync_config.target_table} | batch_size={self.batch_size}"
        )

        try:
            reader = ClickHouseDataReader(
                conn_id=self.source_conn_id,
                batch_size=self.batch_size,
            )
            staging_schema = self._resolve_staging_schema(sync_config)
            delete_missing = sync_config.delete_missing
            scope_column = None
            min_key = None
            max_key = None

            if delete_missing:
                scope_column = self._resolve_delete_scope_column(sync_config)
                min_key, max_key = self._fetch_scope_bounds(
                    reader,
                    sync_config.source_query,
                    scope_column,
                )

            if sync_config.source_query_count is not None:
                total_count = reader.get_total_count(
                    custom_query=sync_config.source_query_count,
                    params=None,
                )
                metrics.total_records = total_count

                if total_count == 0:
                    if delete_missing:
                        writer = self._create_writer()
                        keys_suffix = self._sanitize_staging_suffix(execution_date)
                        keys_staging_table = writer.prepare_keys_staging_table(
                            schema=sync_config.target_schema,
                            table=sync_config.target_table,
                            key_columns=list(sync_config.primary_keys),
                            suffix=keys_suffix,
                            staging_schema=staging_schema,
                        )
                        metrics.deleted += self._finalize_delete_missing(
                            writer=writer,
                            sync_config=sync_config,
                            keys_staging_table=keys_staging_table,
                            scope_column=scope_column,
                            min_key=min_key,
                            max_key=max_key,
                        )

                    metrics.mark_completed()
                    return TransferResult.create_success(
                        records_transferred=0,
                        batch_count=0,
                        duration_seconds=metrics.duration_seconds,
                        total_records=total_count,
                        inserted=0,
                        updated=0,
                        deleted=metrics.deleted,
                    )

            writer = self._create_writer()
            primary_keys = list(sync_config.primary_keys)
            keys_staging_table = None

            if delete_missing:
                keys_suffix = self._sanitize_staging_suffix(execution_date)
                keys_staging_table = writer.prepare_keys_staging_table(
                    schema=sync_config.target_schema,
                    table=sync_config.target_table,
                    key_columns=primary_keys,
                    suffix=keys_suffix,
                    staging_schema=staging_schema,
                )

            batch_number = 0
            for batch in reader.stream_query(
                query=sync_config.source_query,
                count_query=sync_config.source_query_count,
                parameters=None,
            ):
                batch_number += 1
                batch_size = len(batch)

                batch_result = writer.upsert_batch(
                    schema=sync_config.target_schema,
                    table=sync_config.target_table,
                    data=batch,
                    key_columns=primary_keys,
                    batch_size=self.batch_size,
                    delete_missing=False,
                    unique_keys=sync_config.unique_keys,
                    resolve_unique_key_conflicts=sync_config.resolve_unique_key_conflicts,
                    use_hash_change_detection=sync_config.use_hash_change_detection,
                    staging_schema=staging_schema,
                ) or {}

                if delete_missing and keys_staging_table and batch:
                    writer.append_keys_to_staging(
                        keys_staging_table=keys_staging_table,
                        data=batch,
                        key_columns=primary_keys,
                        batch_size=self.batch_size,
                    )

                metrics.inserted += batch_result.get("inserted", 0)
                metrics.updated += batch_result.get("updated", 0)
                metrics.deleted += batch_result.get("deleted", 0)
                metrics.increment_batch(batch_size)

                self.logger.info(
                    f"[ClickHouseToMSSQLQueryOrchestrator.sync_data] Batch {batch_number} | rows={batch_size} | transferred={metrics.transferred_records}"
                )

            if delete_missing and keys_staging_table:
                metrics.deleted += self._finalize_delete_missing(
                    writer=writer,
                    sync_config=sync_config,
                    keys_staging_table=keys_staging_table,
                    scope_column=scope_column,
                    min_key=min_key,
                    max_key=max_key,
                )

            metrics.mark_completed()
            self.logger.info(
                f"[ClickHouseToMSSQLQueryOrchestrator.sync_data] Completed | table={sync_config.source_name} | "
                f"transferred={metrics.transferred_records} | inserted={metrics.inserted} | "
                f"updated={metrics.updated} | deleted={metrics.deleted} | duration={metrics.duration_seconds}s"
            )

            return TransferResult.create_success(
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=(
                    total_count if sync_config.source_query_count is not None else None
                ),
                inserted=metrics.inserted,
                updated=metrics.updated,
                deleted=metrics.deleted,
            )

        except Exception as exc:
            error_message = f"Transfer failed for {sync_config.source_name}: {exc}"
            self.logger.error(
                f"[ClickHouseToMSSQLQueryOrchestrator.sync_data] Failed | table={sync_config.source_name} | error={exc}",
                exc_info=True
            )
            metrics.mark_failed(error_message)
            return TransferResult.create_failure(
                error_message=error_message,
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=metrics.total_records,
                inserted=metrics.inserted,
                updated=metrics.updated,
                deleted=metrics.deleted,
            )
