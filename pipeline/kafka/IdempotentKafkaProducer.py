"""
Idempotent Kafka Producer with exactly-once semantics support.
"""
import logging
from datetime import datetime
from typing import Dict, Optional, Any, List

from confluent_kafka import Producer, KafkaException as ConfluentKafkaException  # type: ignore

from pipeline.interfaces.MessageProducer import MessageProducer
from pipeline.config.KafkaProducerConfig import KAFKA_CONFIG
from pipeline.core.exceptions import KafkaProducerError, KafkaConnectionError
from pipeline.kafka.KafkaConnectionFactory import KafkaConnectionFactory
from pipeline.kafka.MessageSerializer import MessageSerializer


class IdempotentKafkaProducer(MessageProducer):
    """
    Kafka Producer with exactly-once semantics (idempotent producer).

    Builds client config from KafkaConnectionFactory so SASL/SSL and
    bootstrap servers come from the Airflow Connection.
    """

    def __init__(
        self,
        conn_id: str,
        client_id: str,
        enable_idempotence: bool = True,
        acks: str = "all",
        compression_type: str = "snappy",
        max_in_flight_requests_per_connection: int = 5,
        retry_backoff_ms: int = KAFKA_CONFIG.RETRY_BACKOFF_MS,
        message_timeout_ms: int = KAFKA_CONFIG.MESSAGE_TIMEOUT_MS,
        **kwargs,
    ):
        """
        Initialize Kafka Producer with idempotent configuration.

        Args:
            conn_id: Airflow Connection ID for the Kafka cluster
            client_id: Kafka client identifier for this producer

        Raises:
            KafkaConnectionError: If producer initialization fails
        """
        if not conn_id:
            raise ValueError("conn_id cannot be empty.")

        self._closed = False
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conn_id = conn_id
        self.client_id = client_id

        factory = KafkaConnectionFactory(conn_id)
        self.bootstrap_servers = factory.get_bootstrap_servers()

        self.config = factory.get_client_config(
            client_id=client_id,
            **{
                "enable.idempotence": enable_idempotence,
                "acks": acks,
                "compression.type": compression_type,
                "max.in.flight.requests.per.connection": max_in_flight_requests_per_connection,
                "retry.backoff.ms": retry_backoff_ms,
                "message.timeout.ms": message_timeout_ms,
                "queue.buffering.max.messages": KAFKA_CONFIG.MAX_QUEUE_MESSAGES,
                "queue.buffering.max.kbytes": KAFKA_CONFIG.MAX_QUEUE_KB,
                "batch.size": KAFKA_CONFIG.BATCH_SIZE,
                "linger.ms": KAFKA_CONFIG.LINGER_MS,
                **kwargs,
            },
        )

        try:
            self.producer = Producer(self.config)
            self.logger.info(
                f"[IdempotentKafkaProducer.__init__] Kafka producer initialized | conn_id={conn_id} | "
                f"bootstrap_servers={self.bootstrap_servers} | client_id={client_id} | "
                f"enable_idempotence={enable_idempotence}"
            )
        except Exception as e:
            self.logger.error(
                f"[IdempotentKafkaProducer.__init__] Failed to initialize Kafka producer | conn_id={conn_id} | "
                f"bootstrap_servers={self.bootstrap_servers} | client_id={client_id} | error={str(e)}",
                exc_info=True,
            )
            raise KafkaConnectionError(
                f"Failed to initialize Kafka producer: {str(e)}"
            ) from e

        self.delivered_records: List[Dict[str, Any]] = []
        self.failed_records: List[Dict[str, Any]] = []
        self._message_count = 0  # Track messages for periodic polling
        
    def _delivery_callback(self, err, msg) -> None:
        """Callback to receive delivery status."""
        if err:
            self.failed_records.append({
                'error': str(err),
                'topic': msg.topic() if msg else None,
                'timestamp': datetime.now().isoformat()
            })
            self.logger.error(
                f"[IdempotentKafkaProducer._delivery_callback] Message delivery failed | "
                f"topic={msg.topic() if msg else None} | error={str(err)}"
            )
        else:
            self.delivered_records.append({
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'key': msg.key()
            })
    
    def produce(
        self,
        topic: str,
        key: Optional[bytes],
        value: bytes,
        headers: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Produce message to Kafka topic.
        
        Args:
            topic: Kafka topic name
            key: Message key (bytes)
            value: Message value (bytes)
            headers: Optional message headers
        """
        if getattr(self, '_closed', False):
            raise RuntimeError("Cannot produce to a closed producer.")
        
        # Build headers outside try block to ensure availability in except
        kafka_headers = None
        if headers:
            kafka_headers = [(k, str(v).encode('utf-8')) for k, v in headers.items()]
        
        try:
            self.producer.produce(
                topic=topic,
                key=key,
                value=value,
                headers=kafka_headers,
                callback=self._delivery_callback,
                timestamp=int(datetime.now().timestamp() * 1000)
            )
            
            self._message_count += 1
            
            # Poll more frequently to process callbacks and prevent queue full
            # Poll every N messages to clear the delivery queue
            if self._message_count % KAFKA_CONFIG.POLL_INTERVAL == 0:
                self.producer.poll(KAFKA_CONFIG.POLL_TIMEOUT)
            
        except BufferError as e:
            # Queue is full, flush and retry
            self.logger.warning(
                f"[IdempotentKafkaProducer.produce] Producer queue is full, polling to clear delivery queue "
                f"before retry | topic={topic} | client_id={self.client_id} | message_count={self._message_count}"
            )
            self.producer.poll(KAFKA_CONFIG.QUEUE_FULL_POLL_TIMEOUT)
            try:
                self.producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    headers=kafka_headers,
                    callback=self._delivery_callback,
                    timestamp=int(datetime.now().timestamp() * 1000)
                )
            except Exception as retry_error:
                self.logger.error(
                    f"[IdempotentKafkaProducer.produce] Failed to produce message after retry | "
                    f"topic={topic} | error={str(retry_error)}",
                    exc_info=True
                )
                raise KafkaProducerError(f"Producer queue full and retry failed: {str(retry_error)}") from retry_error
        except ConfluentKafkaException as e:
            self.logger.error(
                f"[IdempotentKafkaProducer.produce] Kafka error producing message | topic={topic} | error={str(e)}",
                exc_info=True
            )
            raise KafkaProducerError(f"Kafka produce error: {str(e)}") from e
        except Exception as e:
            self.logger.error(
                f"[IdempotentKafkaProducer.produce] Unexpected error producing message | "
                f"topic={topic} | error={str(e)}",
                exc_info=True
            )
            raise KafkaProducerError(f"Unexpected produce error: {str(e)}") from e
    
    def flush(self, timeout: float = KAFKA_CONFIG.DEFAULT_FLUSH_TIMEOUT) -> None:
        """
        Wait for all messages to be delivered.
        
        Args:
            timeout: Maximum time to wait (seconds)
            
        Raises:
            KafkaProducerError: If flush times out with pending messages
        """
        self.logger.info(
            f"[IdempotentKafkaProducer.flush] Flushing Kafka producer | timeout={timeout} | "
            f"message_count={self._message_count}"
        )
        self.logger.debug(f"[IdempotentKafkaProducer.flush] Flushing producer with timeout={timeout}s")
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            self.logger.error(
                f"[IdempotentKafkaProducer.flush] Producer flush timeout | "
                f"remaining_messages={remaining} | timeout={timeout}"
            )
            self.logger.warning(f"[IdempotentKafkaProducer.flush] {remaining} messages still in queue after flush")
            raise KafkaProducerError(
                f"Producer flush timeout: {remaining} messages still pending after {timeout}s"
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get producer statistics."""
        return {
            'delivered': len(self.delivered_records),
            'failed': len(self.failed_records),
            'delivered_records': self.delivered_records[-10:],  # Last 10
            'failed_records': self.failed_records[-10:]  # Last 10
        }
    
    # def close(self) -> None:
    #     """Close the producer and cleanup resources."""
    #     try:
    #         self.flush(timeout=KAFKA_CONFIG.DEFAULT_FLUSH_TIMEOUT)
    #     except Exception as e:
    #         self.logger.warning(f"Error during producer close: {e}")
    #     finally:
    #         # Producer will be garbage collected
    #         pass

    def close(self, timeout: float = KAFKA_CONFIG.DEFAULT_FLUSH_TIMEOUT) -> None:
        """
        Flush all pending messages and close the producer.

        Args:
            timeout: Max seconds to wait for flush before giving up.
        """
        if getattr(self, "_closed", False):
            return
        try:
            self.flush(timeout)
        except Exception:
            self.logger.error("[IdempotentKafkaProducer.close] Error during producer close", exc_info=True)
            raise
        finally:
            # جلوگیری از استفاده مجدد بعد از close
            self._closed = True
            self.delivered_records.clear()
            self.failed_records.clear()
        
    def _get_key(self, record: Dict[str, Any], key_column: Optional[str]) -> Optional[str]:
        """Determines the key for a record, with auto-detection if key_column is None."""
        if key_column and key_column in record:
            return str(record[key_column])
        elif not key_column and record:
            # Auto-detect: use the first key found in the record's dictionary
            first_key = next(iter(record))
            return str(record[first_key])
        return None # No key found or applicable
    
    def send_batch_to_kafka(
        self,
        batch: list,
        topic: str,
        source_name: str,
        key_column: Optional[str],
        execution_date: str,
        batch_number: int,
        version : int
    ) -> None:
        """
        Send a batch of records to Kafka.

        Args:
            batch: List of row dicts to send
            topic: Kafka topic name
            source_name: Source identifier for headers
            key_column: Column for message key (or None for auto-detect)
            execution_date: Execution date
            batch_number: Batch number for headers
        """
        # Create headers for this batch
        headers = MessageSerializer.create_headers(
            source_table=source_name,
            execution_date=execution_date,
            batch_number=batch_number,
        )

        # Send each record
        for row in batch:
            # Determine key column automatically
            resolved_key_column = self._get_key(row, key_column)
            # Serialize message value
            row['version_id'] = version
            message_value = MessageSerializer.serialize_row(row)
            # Create message key
            message_key = MessageSerializer.create_message_key(row, resolved_key_column)

            # Send record to Kafka
            self.produce(
                topic=topic,
                key=message_key,
                value=message_value,
                headers=headers,
            )

        # Flush to ensure all messages delivered
        self.flush(timeout=60.0)
        # self.logger.debug(f"Batch {batch_number} flushed to Kafka ({len(batch)} records)")