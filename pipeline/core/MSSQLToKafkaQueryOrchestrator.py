"""
Orchestrator for syncing query results from MSSQL to Kafka.

Reads batches via MSSQLDataReader and produces idempotent Kafka messages.
Chunk planning uses SQL Server NTILE on the source (same as MSSQL→ClickHouse/MySQL).
"""
import logging
import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from pipeline.config.MSSQLToKafkaSyncConfig import MSSQLToKafkaSyncConfig
from pipeline.core.TransferMetrics import TransferMetrics
from pipeline.core.TransferResult import TransferResult
from pipeline.database.MSSQLDataReader import MSSQLDataReader
from pipeline.kafka.IdempotentKafkaProducer import IdempotentKafkaProducer
from pipeline.utils.IdentifierValidator import IdentifierValidator
from pipeline.interfaces.SyncOrchestrator import SyncOrchestrator


class MSSQLToKafkaQueryOrchestrator(SyncOrchestrator):
    """
    Read batches from MSSQL and send them to a Kafka topic.

    Notes:
    - ``key_column`` is used as the Kafka message key (and default chunk column).
    - Each row gets a numeric ``version_id`` derived from ``execution_date`` (or now).
    - Topic creation is handled by the DAG factory (``KafkaTopicManager``), not here.
    """

    def __init__(
        self,
        source_conn_id: str,
        kafka_conn_id: str,
        fail_on_error: bool,
        batch_size: int = 5000,
        version_id: Optional[int] = None,
    ) -> None:
        self.source_conn_id = source_conn_id
        self.kafka_conn_id = kafka_conn_id
        self.fail_on_error = fail_on_error
        self.batch_size = batch_size
        self.version_id = version_id
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _resolve_version_id(
        version_id: Optional[int],
        execution_date: Optional[str] = None,
    ) -> int:
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
    def _resolve_chunk_column(sync_config: MSSQLToKafkaSyncConfig) -> str:
        if sync_config.chunk_column:
            return sync_config.chunk_column
        if sync_config.primary_keys:
            return sync_config.primary_keys[0]
        return sync_config.key_column

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
        IdentifierValidator.validate_and_raise(chunk_column, "column name")
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
        IdentifierValidator.validate_and_raise(chunk_column, "column name")
        inner_query = cls._normalize_source_query(source_query)
        return f"""
        SELECT source_data.*
        FROM (
            {inner_query}
        ) AS source_data
        WHERE source_data.[{chunk_column}] >= %s
          AND source_data.[{chunk_column}] <= %s
        """

    def _create_producer(self, source_name: str) -> IdempotentKafkaProducer:
        client_id = f"airflow-{source_name.replace('.', '-')}"
        return IdempotentKafkaProducer(
            conn_id=self.kafka_conn_id,
            client_id=client_id,
        )

    def plan_sync_chunks(
        self,
        sync_config: MSSQLToKafkaSyncConfig,
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
                "[MSSQLToKafkaQueryOrchestrator.plan_sync_chunks] No rows to plan | "
                "source=%s",
                sync_config.source_name,
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
            "[MSSQLToKafkaQueryOrchestrator.plan_sync_chunks] Planned %d chunks | "
            "source=%s | total_rows=%s",
            len(chunks),
            sync_config.source_name,
            total_count,
        )
        return chunks

    def sync_data_chunk(
        self,
        sync_config: MSSQLToKafkaSyncConfig,
        kafka_topic: str,
        chunk: Dict[str, Any],
        execution_date: str,
    ) -> TransferResult:
        """Sync one key-range chunk from MSSQL to Kafka."""
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
            "[MSSQLToKafkaQueryOrchestrator.sync_data_chunk] Starting | source=%s | "
            "topic=%s | chunk=%s | %s=[%s, %s] | expected_rows=%s",
            sync_config.source_name,
            kafka_topic,
            chunk_no,
            chunk_column,
            min_key,
            max_key,
            expected_rows,
        )

        try:
            reader = MSSQLDataReader(
                conn_id=self.source_conn_id,
                batch_size=self.batch_size,
            )
            producer = self._create_producer(sync_config.source_name)

            batch_number = 0
            for batch in reader.stream_query(
                query=chunk_query,
                count_query=None,
                parameters=query_params,
            ):
                batch_number += 1
                batch_size = len(batch)

                try:
                    producer.send_batch_to_kafka(
                        batch=batch,
                        topic=kafka_topic,
                        source_name=sync_config.source_name,
                        key_column=sync_config.key_column,
                        execution_date=execution_date,
                        batch_number=batch_number,
                        version=version_id,
                    )
                except Exception as exc:
                    self.logger.error(
                        "[MSSQLToKafkaQueryOrchestrator.sync_data_chunk] Kafka send "
                        "failed | chunk=%s | batch=%s | error=%s",
                        chunk_no,
                        batch_number,
                        exc,
                    )
                    if self.fail_on_error:
                        raise

                metrics.inserted += batch_size
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
        sync_config: MSSQLToKafkaSyncConfig,
        kafka_topic: str,
        execution_date: str,
    ) -> TransferResult:
        metrics = TransferMetrics(
            table_name=sync_config.source_name,
            execution_date=execution_date,
        )
        total_count = None
        version_id = self._resolve_version_id(self.version_id, execution_date)

        self.logger.info(
            "[MSSQLToKafkaQueryOrchestrator.sync_data] Starting MSSQL → Kafka "
            "sync | source=%s | topic=%s | batch_size=%s | version_id=%s",
            sync_config.source_name,
            kafka_topic,
            self.batch_size,
            version_id,
        )

        try:
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

            producer = self._create_producer(sync_config.source_name)

            batch_number = 0
            for batch in reader.stream_query(
                query=sync_config.source_query,
                count_query=sync_config.source_query_count,
                parameters=None,
            ):
                batch_number += 1
                batch_size = len(batch)

                try:
                    producer.send_batch_to_kafka(
                        batch=batch,
                        topic=kafka_topic,
                        source_name=sync_config.source_name,
                        key_column=sync_config.key_column,
                        execution_date=execution_date,
                        batch_number=batch_number,
                        version=version_id,
                    )
                except Exception as exc:
                    self.logger.error(
                        "[MSSQLToKafkaQueryOrchestrator.sync_data] Kafka send "
                        "failed | batch=%s | error=%s",
                        batch_number,
                        exc,
                    )
                    if self.fail_on_error:
                        raise

                metrics.inserted += batch_size
                metrics.increment_batch(batch_size)

                self.logger.info(
                    "[MSSQLToKafkaQueryOrchestrator.sync_data] Batch %s | rows=%s | "
                    "transferred=%s",
                    batch_number,
                    batch_size,
                    metrics.transferred_records,
                )

            metrics.mark_completed()
            self.logger.info(
                "[MSSQLToKafkaQueryOrchestrator.sync_data] Completed | source=%s | "
                "topic=%s | transferred=%s | duration=%ss",
                sync_config.source_name,
                kafka_topic,
                metrics.transferred_records,
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
                "[MSSQLToKafkaQueryOrchestrator.sync_data] Failed | source=%s | error=%s",
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
