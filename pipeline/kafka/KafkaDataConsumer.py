"""
Kafka Data Consumer — streams topic messages in batches for MSSQL upsert.
"""
import logging
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple

from confluent_kafka import Consumer, KafkaError, TopicPartition  # type: ignore

from pipeline.core.exceptions import KafkaConsumerError, KafkaConnectionError
from pipeline.kafka.KafkaConnectionFactory import KafkaConnectionFactory
from pipeline.kafka.MessageSerializer import MessageSerializer
from pipeline.interfaces.MessageConsumer import MessageConsumer


class KafkaDataConsumer(MessageConsumer):
    """
    Consumes JSON row messages from a Kafka topic in streaming batches.

    Offsets are committed explicitly after the caller confirms successful
    processing (at-least-once). Compatible with messages produced by
    ``IdempotentKafkaProducer`` / ``MessageSerializer.serialize_row``.
    """

    def __init__(
        self,
        conn_id: str,
        consumer_group: str,
        batch_size: int = 10_000,
        auto_offset_reset: str = "earliest",
        poll_timeout_sec: float = 1.0,
        max_idle_polls: int = 10,
        max_messages_per_run: Optional[int] = None,
        session_timeout_ms: int = 45_000,
        max_poll_interval_ms: int = 300_000,
        value_columns: Optional[Sequence[str]] = None,
        exclude_columns: Optional[Sequence[str]] = None,
        client_id: Optional[str] = None,
    ) -> None:
        if not conn_id:
            raise ValueError("conn_id cannot be empty.")
        if not consumer_group:
            raise ValueError("consumer_group cannot be empty.")
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")

        self.logger = logging.getLogger(self.__class__.__name__)
        self.conn_id = conn_id
        self.consumer_group = consumer_group
        self.batch_size = batch_size
        self.auto_offset_reset = auto_offset_reset
        self.poll_timeout_sec = poll_timeout_sec
        self.max_idle_polls = max_idle_polls
        self.max_messages_per_run = max_messages_per_run
        self.session_timeout_ms = session_timeout_ms
        self.max_poll_interval_ms = max_poll_interval_ms
        self.value_columns = tuple(value_columns) if value_columns else None
        self.exclude_columns = tuple(exclude_columns or ())
        self.client_id = client_id or f"kafka-mssql-sync-{consumer_group}"
        self.factory = KafkaConnectionFactory(conn_id)
        self._consumer: Optional[Consumer] = None

    def _build_consumer_config(self) -> Dict[str, Any]:
        return self.factory.get_client_config(
            client_id=self.client_id,
            **{
                "group.id": self.consumer_group,
                "enable.auto.commit": False,
                "auto.offset.reset": self.auto_offset_reset,
                "session.timeout.ms": self.session_timeout_ms,
                "max.poll.interval.ms": self.max_poll_interval_ms,
            },
        )

    def create_consumer(self) -> Consumer:
        """Create a confluent-kafka Consumer for this connection/group."""
        try:
            config = self._build_consumer_config()
            self.logger.debug(
                f'[KafkaDataConsumer.create_consumer] Creating Kafka Consumer | conn_id={self.conn_id} | '
                f'group={self.consumer_group} | bootstrap={config.get("bootstrap.servers")}'
            )
            return Consumer(config)
        except Exception as exc:
            self.logger.error(
                f"[KafkaDataConsumer.create_consumer] Failed to create Kafka Consumer | "
                f"conn_id={self.conn_id} | error={str(exc)}",
                exc_info=True,
            )
            raise KafkaConnectionError(
                f"Failed to create Kafka Consumer for {self.conn_id}: {exc}"
            ) from exc

    @contextmanager
    def get_consumer(self):
        """Context manager that closes the consumer on exit."""
        consumer = None
        try:
            consumer = self.create_consumer()
            self._consumer = consumer
            yield consumer
        finally:
            self._consumer = None
            if consumer is not None:
                try:
                    consumer.close()
                except Exception:
                    self.logger.warning(
                        "[KafkaDataConsumer.get_consumer] Error closing Kafka consumer",
                        exc_info=True,
                    )

    def normalize_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Apply column include/exclude filters for MSSQL upsert."""
        if not isinstance(row, dict):
            raise TypeError(f"Expected dict row, got {type(row).__name__}")

        filtered = dict(row)
        for col in self.exclude_columns:
            filtered.pop(col, None)

        if self.value_columns is not None:
            filtered = {
                col: filtered[col]
                for col in self.value_columns
                if col in filtered
            }
        return filtered

    def _deserialize_message(self, msg) -> Dict[str, Any]:
        value = msg.value()
        if value is None:
            raise KafkaConsumerError(
                f"Null message value at {msg.topic()}[{msg.partition()}]@{msg.offset()}"
            )
        row = MessageSerializer.deserialize_row(value)
        return self.normalize_row(row)

    def _track_offsets(
        self,
        offsets: Dict[Tuple[str, int], TopicPartition],
        msg,
    ) -> None:
        """Store next offset to commit for the message's partition."""
        key = (msg.topic(), msg.partition())
        offsets[key] = TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)

    def commit_offsets(
        self,
        consumer: Consumer,
        offsets: Dict[Tuple[str, int], TopicPartition],
    ) -> None:
        """Commit the highest processed offset per partition (synchronous)."""
        if not offsets:
            return
        try:
            consumer.commit(offsets=list(offsets.values()), asynchronous=False)
            self.logger.debug(f"[KafkaDataConsumer.commit_offsets] Committed Kafka offsets | partitions={len(offsets)}")
        except Exception as exc:
            raise KafkaConsumerError(f"Failed to commit Kafka offsets: {exc}") from exc

    def plan_partition_chunks(self, topic: str) -> List[Dict[str, Any]]:
        """
        List topic partitions for parallel mapped sync tasks.

        Returns list of {chunk_no, partition_id, topic, row_count}.
        ``row_count`` is approximate remaining lag for this consumer group
        when available; otherwise 0.
        """
        if not topic:
            raise ValueError("topic is required")

        metadata = self.factory.list_topics(timeout=10.0)
        if topic not in metadata.topics:
            raise KafkaConsumerError(f"Kafka topic does not exist: {topic}")

        partition_ids = sorted(metadata.topics[topic].partitions.keys())
        if not partition_ids:
            return []

        lag_by_partition: Dict[int, int] = {}
        with self.get_consumer() as consumer:
            tps = [TopicPartition(topic, p) for p in partition_ids]
            try:
                committed = consumer.committed(tps, timeout=10.0)
            except Exception:
                committed = tps

            for tp in committed:
                try:
                    low, high = consumer.get_watermark_offsets(tp, timeout=10.0)
                    committed_offset = tp.offset if tp.offset is not None and tp.offset >= 0 else low
                    lag_by_partition[tp.partition] = max(0, high - committed_offset)
                except Exception:
                    lag_by_partition[tp.partition] = 0

        chunks: List[Dict[str, Any]] = []
        for index, partition_id in enumerate(partition_ids, start=1):
            chunks.append(
                {
                    "chunk_no": index,
                    "partition_id": partition_id,
                    "topic": topic,
                    "row_count": lag_by_partition.get(partition_id, 0),
                }
            )
        return chunks

    def iter_batches(
        self,
        topic: str,
        assigned_partitions: Optional[Sequence[int]] = None,
    ) -> Generator[
        Tuple[
            List[Dict[str, Any]],
            Dict[Tuple[str, int], TopicPartition],
            Consumer,
        ],
        None,
        None,
    ]:
        """
        Stream deserialized row batches with a live consumer for offset commits.

        Yields:
            (batch_rows, offsets_to_commit, consumer) — upsert then
            ``commit_offsets(consumer, offsets)`` before the next batch.
        """
        with self.get_consumer() as consumer:
            for batch, offsets in self._stream_with_consumer(
                topic=topic,
                assigned_partitions=assigned_partitions,
                consumer=consumer,
            ):
                yield batch, offsets, consumer

    def stream_and_process(
        self,
        topic: str,
        process_batch,
        assigned_partitions: Optional[Sequence[int]] = None,
    ) -> int:
        """
        Consume batches, invoke ``process_batch(rows)``, then commit offsets.

        Args:
            topic: Kafka topic name
            process_batch: Callable taking List[Dict] (must raise on failure)
            assigned_partitions: Optional partition subset (manual assign)

        Returns:
            Total rows processed
        """
        processed = 0
        for batch, offsets, consumer in self.iter_batches(
            topic=topic,
            assigned_partitions=assigned_partitions,
        ):
            process_batch(batch)
            self.commit_offsets(consumer, offsets)
            processed += len(batch)
        return processed

    def _subscribe_or_assign(
        self,
        consumer: Consumer,
        topic: str,
        assigned_partitions: Optional[Sequence[int]],
    ) -> None:
        if assigned_partitions is not None:
            tps = [TopicPartition(topic, int(p)) for p in assigned_partitions]
            consumer.assign(tps)
            self.logger.info(
                f"[KafkaDataConsumer._subscribe_or_assign] Assigned partitions | topic={topic} | partitions={list(assigned_partitions)}"
            )
        else:
            consumer.subscribe([topic])
            self.logger.info(
                f"[KafkaDataConsumer._subscribe_or_assign] Subscribed | topic={topic} | group={self.consumer_group}"
            )

    def _stream_with_consumer(
        self,
        topic: str,
        assigned_partitions: Optional[Sequence[int]],
        consumer: Optional[Consumer],
    ) -> Generator[Tuple[List[Dict[str, Any]], Dict[Tuple[str, int], TopicPartition]], None, None]:
        if not topic:
            raise ValueError("topic is required")

        owns_consumer = consumer is None
        if owns_consumer:
            consumer = self.create_consumer()
            self._consumer = consumer

        batch: List[Dict[str, Any]] = []
        offsets: Dict[Tuple[str, int], TopicPartition] = {}
        processed = 0
        batch_number = 0
        idle_polls = 0
        reached_limit = False

        try:
            self._subscribe_or_assign(consumer, topic, assigned_partitions)

            self.logger.info(
                f"[KafkaDataConsumer._stream_with_consumer] START | topic={topic} | group={self.consumer_group} | "
                f"batch_size={self.batch_size} | max_messages={self.max_messages_per_run}"
            )

            while True:
                if (
                    self.max_messages_per_run is not None
                    and processed >= self.max_messages_per_run
                ):
                    reached_limit = True
                    break

                msg = consumer.poll(self.poll_timeout_sec)
                if msg is None:
                    idle_polls += 1
                    if batch:
                        batch_number += 1
                        self.logger.info(
                            f"[KafkaDataConsumer._stream_with_consumer] Batch {batch_number} | rows={len(batch)} | processed={processed:,} (flush on idle)"
                        )
                        yield batch, dict(offsets)
                        batch = []
                        offsets = {}
                    if idle_polls >= self.max_idle_polls:
                        self.logger.info(
                            f"[KafkaDataConsumer._stream_with_consumer] Idle limit reached | idle_polls={idle_polls}"
                        )
                        break
                    continue

                idle_polls = 0
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaConsumerError(
                        f"Kafka consume error: {msg.error()}"
                    )

                try:
                    row = self._deserialize_message(msg)
                except Exception as exc:
                    raise KafkaConsumerError(
                        f"Failed to deserialize message at "
                        f"{msg.topic()}[{msg.partition()}]@{msg.offset()}: {exc}"
                    ) from exc

                batch.append(row)
                self._track_offsets(offsets, msg)
                processed += 1

                if len(batch) >= self.batch_size:
                    batch_number += 1
                    self.logger.info(
                        f"[KafkaDataConsumer._stream_with_consumer] Batch {batch_number} | rows={len(batch)} | processed={processed:,}"
                    )
                    yield batch, dict(offsets)
                    batch = []
                    offsets = {}

                if (
                    self.max_messages_per_run is not None
                    and processed >= self.max_messages_per_run
                ):
                    reached_limit = True
                    break

            if batch:
                batch_number += 1
                self.logger.info(
                    f"[KafkaDataConsumer._stream_with_consumer] Batch {batch_number} | rows={len(batch)} | processed={processed:,} (final flush)"
                )
                yield batch, dict(offsets)

            self.logger.info(
                f"[KafkaDataConsumer._stream_with_consumer] FINISH | processed={processed} | "
                f"batches={batch_number} | reached_limit={reached_limit}"
            )
        except KafkaConsumerError:
            raise
        except Exception as exc:
            self.logger.error(
                f"[KafkaDataConsumer._stream_with_consumer] ERROR | processed={processed} | error={exc}",
                exc_info=True
            )
            raise KafkaConsumerError(f"Streaming Kafka topic failed: {exc}") from exc
        finally:
            if owns_consumer and consumer is not None:
                self._consumer = None
                try:
                    consumer.close()
                except Exception:
                    self.logger.warning(
                        "[KafkaDataConsumer._stream_with_consumer] Error closing Kafka consumer after stream",
                        exc_info=True,
                    )
