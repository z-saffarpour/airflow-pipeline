"""
Orchestrator for syncing Kafka topic data to MSSQL (upsert).
"""
import logging
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from pipeline.config.KafkaSyncConfig import KafkaSyncConfig
from pipeline.core.TransferMetrics import TransferMetrics
from pipeline.core.TransferResult import TransferResult
from pipeline.database.MSSQLServerWriter import MSSQLServerWriter
from pipeline.kafka.KafkaDataConsumer import KafkaDataConsumer
from pipeline.utils.IdentifierValidator import IdentifierValidator
from pipeline.interfaces.SyncOrchestrator import SyncOrchestrator


class KafkaToMSSQLQueryOrchestrator(SyncOrchestrator):
    """
    Consume batches from Kafka and upsert into MSSQL target tables.
    Mirrors MySQL/Mongo → MSSQL orchestrators with Kafka source semantics.

    Delivery semantics: at-least-once (offset commit after successful MERGE).
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
    def _resolve_delete_scope_column(sync_config: KafkaSyncConfig) -> str:
        if sync_config.delete_scope_column:
            return sync_config.delete_scope_column
        if sync_config.primary_keys:
            return sync_config.primary_keys[0]
        raise ValueError(
            "delete_scope_column or primary_keys is required when delete_missing=True"
        )

    @staticmethod
    def _resolve_staging_schema(sync_config: KafkaSyncConfig) -> Optional[str]:
        schema = sync_config.staging_schema
        if schema:
            IdentifierValidator.validate_and_raise(schema, "staging schema")
        return schema

    @staticmethod
    def _sanitize_staging_suffix(value: str) -> str:
        return re.sub(r"[^\w]", "_", str(value))[:50]

    def _create_consumer(
        self,
        sync_config: KafkaSyncConfig,
        client_id_suffix: Optional[str] = None,
    ) -> KafkaDataConsumer:
        client_id = f"kafka-mssql-sync-{sync_config.consumer_group}"
        if client_id_suffix:
            client_id = f"{client_id}-{client_id_suffix}"

        return KafkaDataConsumer(
            conn_id=self.source_conn_id,
            consumer_group=sync_config.consumer_group,
            batch_size=self.batch_size,
            auto_offset_reset=sync_config.auto_offset_reset,
            poll_timeout_sec=sync_config.poll_timeout_sec,
            max_idle_polls=sync_config.max_idle_polls,
            max_messages_per_run=sync_config.max_messages_per_run,
            session_timeout_ms=sync_config.session_timeout_ms,
            max_poll_interval_ms=sync_config.max_poll_interval_ms,
            value_columns=sync_config.value_columns,
            exclude_columns=sync_config.exclude_columns,
            client_id=client_id,
        )

    def _create_writer(self) -> MSSQLServerWriter:
        return MSSQLServerWriter(
            conn_id=self.target_conn_id,
            is_connection_string=self.target_is_connection_string,
        )

    def _finalize_delete_missing(
        self,
        writer: MSSQLServerWriter,
        sync_config: KafkaSyncConfig,
        keys_staging_table: str,
        scope_column: str,
        min_key: Any,
        max_key: Any,
    ) -> int:
        try:
            if min_key is None or max_key is None:
                self.logger.info(
                    f"[KafkaToMSSQLQueryOrchestrator._finalize_delete_missing] Skipping scoped delete | "
                    f"table={sync_config.target_table} | reason=empty consumed scope bounds"
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
                f"[KafkaToMSSQLQueryOrchestrator._finalize_delete_missing] Scoped delete completed | "
                f"table={sync_config.target_table} | deleted={deleted}"
            )
            return deleted
        finally:
            writer.drop_keys_staging_table(keys_staging_table)

    @staticmethod
    def _update_key_bounds(
        min_key: Any,
        max_key: Any,
        batch: List[Dict[str, Any]],
        scope_column: str,
    ) -> Tuple[Any, Any]:
        for row in batch:
            value = row.get(scope_column)
            if value is None:
                continue
            if min_key is None or value < min_key:
                min_key = value
            if max_key is None or value > max_key:
                max_key = value
        return min_key, max_key

    def plan_sync_chunks(
        self,
        sync_config: KafkaSyncConfig,
    ) -> List[Dict[str, Any]]:
        """Build one chunk per Kafka partition for parallel dynamic sync tasks."""
        consumer = self._create_consumer(sync_config)
        chunks = consumer.plan_partition_chunks(sync_config.kafka_topic)
        self.logger.info(
            f'[KafkaToMSSQLQueryOrchestrator.plan_sync_chunks] Planned {len(chunks)} partition chunks | '
            f'topic={sync_config.kafka_topic} | approx_lag={sum(chunk.get("row_count", 0) for chunk in chunks)}'
        )
        return chunks

    def _sync_consume(
        self,
        sync_config: KafkaSyncConfig,
        execution_date: str,
        metrics: TransferMetrics,
        assigned_partitions: Optional[Sequence[int]] = None,
        staging_suffix: Optional[str] = None,
        client_id_suffix: Optional[str] = None,
    ) -> TransferResult:
        consumer = self._create_consumer(sync_config, client_id_suffix=client_id_suffix)
        writer = self._create_writer()
        staging_schema = self._resolve_staging_schema(sync_config)
        primary_keys = list(sync_config.primary_keys or ())
        delete_missing = sync_config.delete_missing
        keys_staging_table = None
        scope_column = None
        min_key = None
        max_key = None
        suffix = staging_suffix or self._sanitize_staging_suffix(execution_date)

        if delete_missing:
            scope_column = self._resolve_delete_scope_column(sync_config)
            keys_staging_table = writer.prepare_keys_staging_table(
                schema=sync_config.target_schema,
                table=sync_config.target_table,
                key_columns=primary_keys,
                suffix=suffix,
                staging_schema=staging_schema,
            )

        batch_number = 0
        for batch, offsets, kafka_consumer in consumer.iter_batches(
            topic=sync_config.kafka_topic,
            assigned_partitions=assigned_partitions,
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
                staging_suffix=f"upsert_{suffix}",
            ) or {}

            if delete_missing and keys_staging_table and batch:
                writer.append_keys_to_staging(
                    keys_staging_table=keys_staging_table,
                    data=batch,
                    key_columns=primary_keys,
                    batch_size=self.batch_size,
                )
                min_key, max_key = self._update_key_bounds(
                    min_key,
                    max_key,
                    batch,
                    scope_column,
                )

            # Commit only after successful MSSQL upsert (at-least-once).
            consumer.commit_offsets(kafka_consumer, offsets)

            metrics.inserted += batch_result.get("inserted", 0)
            metrics.updated += batch_result.get("updated", 0)
            metrics.deleted += batch_result.get("deleted", 0)
            metrics.increment_batch(batch_size)

            self.logger.info(
                '[KafkaToMSSQLQueryOrchestrator._sync_consume] Batch %s | rows=%s | transferred=%s',
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
        return TransferResult.create_success(
            records_transferred=metrics.transferred_records,
            batch_count=metrics.batch_count,
            duration_seconds=metrics.duration_seconds,
            total_records=metrics.transferred_records,
            inserted=metrics.inserted,
            updated=metrics.updated,
            deleted=metrics.deleted,
        )

    def sync_data_chunk(
        self,
        sync_config: KafkaSyncConfig,
        chunk: Dict[str, Any],
        execution_date: str,
    ) -> TransferResult:
        """Sync one Kafka partition chunk to MSSQL."""
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

        partition_id = chunk["partition_id"]
        metrics = TransferMetrics(
            table_name=f"{sync_config.source_name}_part_{partition_id}",
            execution_date=execution_date,
        )
        metrics.total_records = chunk.get("row_count", 0)

        self.logger.info(
            f'[KafkaToMSSQLQueryOrchestrator.sync_data_chunk] Starting | table={sync_config.target_table} | '
            f'chunk={chunk_no} | partition={partition_id} | approx_lag={chunk.get("row_count")}'
        )

        try:
            return self._sync_consume(
                sync_config=sync_config,
                execution_date=execution_date,
                metrics=metrics,
                assigned_partitions=[int(partition_id)],
                staging_suffix=f"chunk_{chunk_no}",
                client_id_suffix=f"p{partition_id}",
            )
        except Exception as exc:
            error_message = (
                f"Chunk sync failed for {sync_config.source_name} "
                f"chunk={chunk_no} partition={partition_id}: {exc}"
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
        sync_config: KafkaSyncConfig,
        execution_date: str,
    ) -> TransferResult:
        metrics = TransferMetrics(
            table_name=sync_config.source_name,
            execution_date=execution_date,
        )

        self.logger.info(
            f"[KafkaToMSSQLQueryOrchestrator.sync_data] Starting Kafka → MSSQL sync | "
            f"source={self.source_conn_id} | topic={sync_config.kafka_topic} | "
            f"group={sync_config.consumer_group} | "
            f"target={sync_config.target_schema}.{sync_config.target_table} | batch_size={self.batch_size}"
        )

        try:
            result = self._sync_consume(
                sync_config=sync_config,
                execution_date=execution_date,
                metrics=metrics,
            )
            self.logger.info(
                f"[KafkaToMSSQLQueryOrchestrator.sync_data] Completed | table={sync_config.source_name} | "
                f"transferred={result.records_transferred} | inserted={result.inserted} | updated={result.updated} | "
                f"deleted={result.deleted} | duration={result.duration_seconds}s"
            )
            return result
        except Exception as exc:
            error_message = f"Transfer failed for {sync_config.source_name}: {exc}"
            self.logger.error(
                f"[KafkaToMSSQLQueryOrchestrator.sync_data] Failed | table={sync_config.source_name} | error={exc}",
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
