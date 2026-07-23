"""
Orchestrator for syncing MongoDB collection data to MSSQL (upsert/replication-style).
"""
import logging
import math
import re
from typing import Any, Dict, List, Optional, Tuple

from pipeline.config.MongoSyncConfig import MongoSyncConfig
from pipeline.core.TransferMetrics import TransferMetrics
from pipeline.core.TransferResult import TransferResult
from pipeline.database.MongoDBDataReader import MongoDBDataReader
from pipeline.database.MSSQLServerWriter import MSSQLServerWriter
from pipeline.database.SQLQueryBuilder import SQLQueryBuilder


class MongoDBToMSSQLQueryOrchestrator:
    """
    Read batches from MongoDB and upsert into MSSQL target tables.
    Mirrors MySQLToMSSQLQueryOrchestrator behaviour with MongoDB source semantics.
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
    def _resolve_chunk_column(sync_config: MongoSyncConfig) -> str:
        if sync_config.chunk_column:
            return sync_config.chunk_column
        if sync_config.rename_id_to:
            return sync_config.rename_id_to
        if sync_config.primary_keys:
            return sync_config.primary_keys[0]
        raise ValueError(
            "chunk_column, rename_id_to, or primary_keys is required for dynamic sync tasks"
        )

    @staticmethod
    def _mongo_chunk_field(sync_config: MongoSyncConfig, chunk_column: str) -> str:
        """Map logical/MSSQL column name back to Mongo field (e.g. id → _id)."""
        if sync_config.rename_id_to and chunk_column == sync_config.rename_id_to:
            return "_id"
        return chunk_column

    @staticmethod
    def _resolve_delete_scope_column(sync_config: MongoSyncConfig) -> str:
        if sync_config.delete_scope_column:
            return sync_config.delete_scope_column
        if sync_config.chunk_column:
            return sync_config.chunk_column
        if sync_config.rename_id_to:
            return sync_config.rename_id_to
        raise ValueError(
            "delete_scope_column or chunk_column is required when delete_missing=True"
        )

    @staticmethod
    def _resolve_staging_schema(sync_config: MongoSyncConfig) -> Optional[str]:
        schema = sync_config.staging_schema
        if schema:
            SQLQueryBuilder._validate_and_raise(schema, "staging schema")
        return schema

    @staticmethod
    def _sanitize_staging_suffix(value: str) -> str:
        return re.sub(r"[^\w]", "_", str(value))[:50]

    def _create_reader(self, sync_config: MongoSyncConfig) -> MongoDBDataReader:
        return MongoDBDataReader(
            conn_id=self.source_conn_id,
            batch_size=self.batch_size,
            database=sync_config.database,
            rename_id_to=sync_config.rename_id_to,
            serialize_nested=sync_config.serialize_nested,
        )

    def _create_writer(self) -> MSSQLServerWriter:
        return MSSQLServerWriter(
            conn_id=self.target_conn_id,
            is_connection_string=self.target_is_connection_string,
        )

    def _fetch_scope_bounds(
        self,
        reader: MongoDBDataReader,
        sync_config: MongoSyncConfig,
        scope_column: str,
    ) -> Tuple[Any, Any]:
        mongo_field = self._mongo_chunk_field(sync_config, scope_column)
        return reader.get_field_bounds(
            collection=sync_config.collection,
            field=mongo_field,
            filter_query=sync_config.filter_query,
            database=sync_config.database,
        )

    def _finalize_delete_missing(
        self,
        writer: MSSQLServerWriter,
        sync_config: MongoSyncConfig,
        keys_staging_table: str,
        scope_column: str,
        min_key: Any,
        max_key: Any,
    ) -> int:
        try:
            if min_key is None or max_key is None:
                self.logger.info(
                    "[MongoDBToMSSQLQueryOrchestrator._finalize_delete_missing] Skipping "
                    "scoped delete | table=%s | reason=empty source scope bounds",
                    sync_config.target_table,
                )
                return 0

            # ObjectId bounds may still be ObjectId if taken before normalize;
            # writer expects comparable MSSQL key values — coerce to str when needed.
            if hasattr(min_key, "__class__") and min_key.__class__.__name__ == "ObjectId":
                min_key = str(min_key)
            if hasattr(max_key, "__class__") and max_key.__class__.__name__ == "ObjectId":
                max_key = str(max_key)

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
                "[MongoDBToMSSQLQueryOrchestrator._finalize_delete_missing] Scoped delete "
                "completed | table=%s | deleted=%s",
                sync_config.target_table,
                deleted,
            )
            return deleted
        finally:
            writer.drop_keys_staging_table(keys_staging_table)

    def plan_sync_chunks(
        self,
        sync_config: MongoSyncConfig,
    ) -> List[Dict[str, Any]]:
        """Build key ranges for parallel dynamic sync tasks via $bucketAuto."""
        chunk_column = self._resolve_chunk_column(sync_config)
        mongo_field = self._mongo_chunk_field(sync_config, chunk_column)
        reader = self._create_reader(sync_config)

        total_count = reader.get_total_count(
            collection=sync_config.collection,
            filter_query=sync_config.filter_query,
            database=sync_config.database,
        )
        if total_count == 0:
            self.logger.info(
                "[MongoDBToMSSQLQueryOrchestrator.plan_sync_chunks] No rows to plan | "
                "table=%s",
                sync_config.target_table,
            )
            return []

        chunk_count = max(1, math.ceil(total_count / sync_config.task_chunk_size))
        chunks = reader.plan_bucket_chunks(
            collection=sync_config.collection,
            chunk_column=mongo_field,
            chunk_count=chunk_count,
            filter_query=sync_config.filter_query,
            database=sync_config.database,
        )

        # Expose logical chunk_column used by MSSQL/delete scope naming.
        for chunk in chunks:
            chunk["chunk_column"] = chunk_column
            chunk["mongo_chunk_field"] = mongo_field
            # Normalize ObjectId bounds to str for XCom / MSSQL key compare.
            if hasattr(chunk.get("min_key"), "__class__") and (
                chunk["min_key"].__class__.__name__ == "ObjectId"
            ):
                chunk["min_key"] = str(chunk["min_key"])
            if hasattr(chunk.get("max_key"), "__class__") and (
                chunk["max_key"].__class__.__name__ == "ObjectId"
            ):
                chunk["max_key"] = str(chunk["max_key"])

        self.logger.info(
            "[MongoDBToMSSQLQueryOrchestrator.plan_sync_chunks] Planned %d chunks | "
            "table=%s | total_rows=%s",
            len(chunks),
            sync_config.target_table,
            total_count,
        )
        return chunks

    def sync_data_chunk(
        self,
        sync_config: MongoSyncConfig,
        chunk: Dict[str, Any],
        execution_date: str,
    ) -> TransferResult:
        """Sync one key-range chunk from MongoDB to MSSQL."""
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
        mongo_field = chunk.get("mongo_chunk_field") or self._mongo_chunk_field(
            sync_config,
            chunk_column,
        )
        min_key = chunk["min_key"]
        max_key = chunk["max_key"]
        expected_rows = chunk.get("row_count", 0)

        metrics = TransferMetrics(
            table_name=f"{sync_config.source_name}_chunk_{chunk_no}",
            execution_date=execution_date,
        )
        metrics.total_records = expected_rows

        self.logger.info(
            "[MongoDBToMSSQLQueryOrchestrator.sync_data_chunk] Starting | table=%s | "
            "chunk=%s | %s=[%s, %s] | expected_rows=%s",
            sync_config.target_table,
            chunk_no,
            mongo_field,
            min_key,
            max_key,
            expected_rows,
        )

        try:
            # Restore ObjectId for range filter when chunking on _id.
            range_min, range_max = min_key, max_key
            if mongo_field == "_id":
                try:
                    from bson import ObjectId  # type: ignore

                    if isinstance(min_key, str) and ObjectId.is_valid(min_key):
                        range_min = ObjectId(min_key)
                    if isinstance(max_key, str) and ObjectId.is_valid(max_key):
                        range_max = ObjectId(max_key)
                except ImportError:
                    pass

            reader = self._create_reader(sync_config)
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
            for batch in reader.stream_collection(
                collection=sync_config.collection,
                filter_query=sync_config.filter_query,
                projection=sync_config.projection,
                sort=sync_config.sort,
                database=sync_config.database,
                range_column=mongo_field,
                min_key=range_min,
                max_key=range_max,
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
                scoped_deleted = self._finalize_delete_missing(
                    writer=writer,
                    sync_config=sync_config,
                    keys_staging_table=keys_staging_table,
                    scope_column=delete_scope_column,
                    min_key=min_key,
                    max_key=max_key,
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
        sync_config: MongoSyncConfig,
        execution_date: str,
    ) -> TransferResult:
        metrics = TransferMetrics(
            table_name=sync_config.source_name,
            execution_date=execution_date,
        )
        total_count = None

        self.logger.info(
            "[MongoDBToMSSQLQueryOrchestrator.sync_data] Starting MongoDB → MSSQL sync | "
            "source=%s | collection=%s | target=%s.%s | batch_size=%s",
            self.source_conn_id,
            sync_config.collection,
            sync_config.target_schema,
            sync_config.target_table,
            self.batch_size,
        )

        try:
            reader = self._create_reader(sync_config)
            staging_schema = self._resolve_staging_schema(sync_config)
            delete_missing = sync_config.delete_missing
            scope_column = None
            min_key = None
            max_key = None

            if delete_missing:
                scope_column = self._resolve_delete_scope_column(sync_config)
                min_key, max_key = self._fetch_scope_bounds(
                    reader,
                    sync_config,
                    scope_column,
                )

            total_count = reader.get_total_count(
                collection=sync_config.collection,
                filter_query=sync_config.filter_query,
                database=sync_config.database,
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
            for batch in reader.stream_collection(
                collection=sync_config.collection,
                filter_query=sync_config.filter_query,
                projection=sync_config.projection,
                sort=sync_config.sort,
                database=sync_config.database,
                aggregation_pipeline=sync_config.aggregation_pipeline,
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
                    "[MongoDBToMSSQLQueryOrchestrator.sync_data] Batch %s | rows=%s | "
                    "transferred=%s",
                    batch_number,
                    batch_size,
                    metrics.transferred_records,
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
                "[MongoDBToMSSQLQueryOrchestrator.sync_data] Completed | table=%s | "
                "transferred=%s | inserted=%s | updated=%s | deleted=%s | duration=%ss",
                sync_config.source_name,
                metrics.transferred_records,
                metrics.inserted,
                metrics.updated,
                metrics.deleted,
                metrics.duration_seconds,
            )

            return TransferResult.create_success(
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=total_count,
                inserted=metrics.inserted,
                updated=metrics.updated,
                deleted=metrics.deleted,
            )

        except Exception as exc:
            error_message = f"Transfer failed for {sync_config.source_name}: {exc}"
            self.logger.error(
                "[MongoDBToMSSQLQueryOrchestrator.sync_data] Failed | table=%s | error=%s",
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
                updated=metrics.updated,
                deleted=metrics.deleted,
            )
