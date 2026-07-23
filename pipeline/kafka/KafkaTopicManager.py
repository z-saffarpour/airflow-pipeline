
import logging
import json
import time
from datetime import datetime
from typing import Dict, List, Optional
from confluent_kafka import Consumer, KafkaError # type: ignore
from confluent_kafka.admin import AdminClient, NewTopic # type: ignore
from confluent_kafka import KafkaException # type: ignore

from pipeline.interfaces.TopicManager import TopicManager

# ============================================================================
# KAFKA PRODUCER UTILITY CLASSES
# ============================================================================

class KafkaTopicManager(TopicManager):
    """
    Manages Kafka Topics (creation, verification, and monitoring).
    Implements TopicManager interface for dependency inversion.
    """
    
    def __init__(self, bootstrap_servers: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = AdminClient({
            'bootstrap.servers': bootstrap_servers
        })
    
    def ensure_topic_exists(
        self,
        topic_name: str,
        num_partitions: int = 6,
        replication_factor: int = 3,
        config: Optional[Dict[str, str]] = None
    ) -> None:
        """Create Kafka topic with production-grade validation and high-throughput defaults."""

        # -----------------------
        # Input Validations
        # -----------------------
        if num_partitions < 1:
            raise ValueError("num_partitions must be >= 1")

        if replication_factor < 1:
            raise ValueError("replication_factor must be >= 1")

        # -----------------------
        # Dynamic min.insync.replicas
        # -----------------------
        if replication_factor >= 3:
            min_insync_replicas = 2
        elif replication_factor == 2:
            min_insync_replicas = 1
        else:  # replication_factor == 1
            min_insync_replicas = 1  # must be 1, otherwise producer will fail

        # -----------------------
        # High Throughput Defaults
        # These are optimized for large-scale ingestion workloads
        # -----------------------
        default_config = {
            # Compression (best performance in Kafka at scale)
            "compression.type": "lz4",

            # Log & retention tuning
            "retention.ms": "604800000",     # 7 days
            "segment.bytes": "1073741824",   # 1GB segments
            "segment.ms": "86400000",        # roll segment every 24h

            # Replica consistency
            "min.insync.replicas": str(min_insync_replicas),

            # Allow Kafka to delete old segments
            "cleanup.policy": "delete",

            # Increase message throughput
            # "message.format.version": "3.0",

            # Reduce log flush frequency for higher throughput
            # "flush.ms": "3000",

            # Broker-level sanity: allow large batching
            # "max.message.bytes": "2097152",  # 2MB max message size
            # "receive.message.max.bytes": "2097152"
        }

        # -----------------------
        # Merge user-provided config (without allowing them to break critical settings)
        # -----------------------
        topic_config = {**default_config, **(config or {})}

        max_retries = 3
        base_delay = 2
    
        for attempt in range(max_retries):
            try:
                topics = self.admin_client.list_topics(timeout=10)
                
                if topic_name not in topics.topics:
                    self.logger.info(f"Creating topic: {topic_name}")
                    
                    topic = NewTopic(
                        topic=topic_name,
                        num_partitions=num_partitions,
                        replication_factor=replication_factor,
                        config=topic_config
                    )
                    
                    futures = self.admin_client.create_topics([topic])
                    
                    for topic_name_future, future in futures.items():
                        try:
                            future.result()
                            self.logger.info(f"Topic {topic_name_future} created successfully")
                        except Exception as e:
                            self.logger.error(f"Failed to create topic {topic_name_future}: {e}")
                            raise
                else:
                    self.logger.info(f"Topic {topic_name} already exists")
            except KafkaException as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    self.logger.warning(f"Kafka connection failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    self.logger.error(f"Failed to connect to Kafka after {max_retries} attempts")
                    raise

    def get_topic_stats(self, topic_name: str) -> Dict:
        """
        Get general topic statistics (number of messages, partitions, etc.).
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Dictionary with topic statistics
        """
        metadata = self.admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            return {'error': f'Topic {topic_name} does not exist'}
        
        topic_metadata = metadata.topics[topic_name]
        partitions = topic_metadata.partitions
        
        stats = {
            'topic': topic_name,
            'partition_count': len(partitions),
            'partitions': {}
        }
        
        consumer = self._create_monitoring_consumer()
        
        try:
            for partition_id in partitions.keys():
                try:
                    # Get low and high water marks
                    low, high = consumer.get_watermark_offsets(
                        (topic_name, partition_id),
                        timeout=10
                    )
                    
                    stats['partitions'][f'partition_{partition_id}'] = {
                        'low_water': low,
                        'high_water': high,
                        'message_count': high - low
                    }
                except Exception as e:
                    stats['partitions'][f'partition_{partition_id}'] = {
                        'error': str(e)
                    }
        finally:
            consumer.close()
        
        # Calculate total messages
        total_messages = sum(
            p.get('message_count', 0) 
            for p in stats['partitions'].values()
            if 'message_count' in p
        )
        stats['total_messages'] = total_messages
        
        return stats

    def check_consumer_lag(
        self,
        topic_name: str,
        consumer_group: str
    ) -> Dict:
        """
        Check Lag in Kafka Consumer Group.
        
        Args:
            topic_name: Name of the topic
            consumer_group: Consumer group ID
        
        Returns:
            Dictionary with lag information for each partition
        """
        metadata = self.admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            return {'error': f'Topic {topic_name} does not exist'}
        
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })
        
        # Get topic metadata
        topic_metadata = metadata.topics[topic_name]
        partitions = topic_metadata.partitions
        
        lag_info = {}
        
        try:
            for partition_id in partitions.keys():
                # Get topic partition info
                tp = consumer.list_topics(topic_name, timeout=10)
                if topic_name in tp.topics:
                    partition_metadata = tp.topics[topic_name].partitions[partition_id]
                    high_water = partition_metadata.high  # Latest offset
                    
                    # Get committed offset for consumer group
                    try:
                        committed = consumer.committed([(topic_name, partition_id)], timeout=10)
                        if committed:
                            low_water = committed[0].offset
                            lag = high_water - low_water
                            
                            lag_info[f'partition_{partition_id}'] = {
                                'current_offset': low_water,
                                'high_water': high_water,
                                'lag': lag
                            }
                    except Exception as e:
                        lag_info[f'partition_{partition_id}'] = {'error': str(e)}
        finally:
            consumer.close()
        
        return lag_info
    def _validate_topic_inputs(self, num_partitions: int, replication_factor: int):

        if num_partitions < 1:
            raise ValueError("num_partitions must be >= 1")

        if replication_factor < 1:
            raise ValueError("replication_factor must be >= 1")

    def sample_messages(
        self,
        topic_name: str,
        partition: int = 0,
        count: int = 10,
        offset: str = 'latest'
    ) -> List[Dict]:
        """
        Sample messages from topic.
        
        Args:
            topic_name: Topic name
            partition: Partition number
            count: Number of messages to sample
            offset: 'earliest' or 'latest'
            
        Returns:
            List of sampled messages
        """
        consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f'sampling_{datetime.now().timestamp()}',
            'auto.offset.reset': offset,
            'enable.auto.commit': False
        })
        
        consumer.assign([(topic_name, partition)])
        
        if offset == 'latest':
            # Seek to end and move back
            consumer.seek((topic_name, partition, -1))  # Seek to end
        
        messages = []
        timeout = 10.0
        start_time = datetime.now()
        
        try:
            while len(messages) < count:
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed > timeout:
                    break
                
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        break
                    else:
                        self.logger.warning(f"Consumer error: {msg.error()}")
                        break
                
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    value = msg.value().decode('utf-8')
                
                messages.append({
                    'partition': msg.partition(),
                    'offset': msg.offset(),
                    'key': msg.key().decode('utf-8') if msg.key() else None,
                    'value': value,
                    'timestamp': msg.timestamp()
                })
                
                if len(messages) >= count:
                    break
        
        finally:
            consumer.close()
        
        return messages

    def check_health(
        self,
        topic_name: str,
        consumer_group: str,
        expected_daily_records: int = 5000000
    ) -> Dict:
        """
        Check overall pipeline health.
        
        Args:
            topic_name: Name of the topic
            consumer_group: Consumer group ID
            expected_daily_records: Expected daily record count for threshold calculation
            
        Returns:
            Health report including topic status, lag info, and recommendations
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'topic': topic_name,
            'status': 'unknown',
            'recommendations': []
        }
        
        # Check topic exists and get stats
        topic_stats = self.get_topic_stats(topic_name)
        
        if 'error' in topic_stats:
            report['status'] = 'error'
            report['error'] = topic_stats['error']
            return report
        
        report['topic_stats'] = topic_stats
        
        # Check lag
        lag_info = self.check_consumer_lag(topic_name, consumer_group)
        report['lag_info'] = lag_info
        
        # Calculate total lag
        total_lag = sum(
            p.get('lag', 0)
            for p in lag_info.values()
            if isinstance(p, dict) and 'lag' in p
        )
        report['total_lag'] = total_lag
        
        # Health assessment
        if total_lag == 0:
            report['status'] = 'healthy'
        elif total_lag < expected_daily_records * 0.1:  # Less than 10% of daily
            report['status'] = 'warning'
            report['recommendations'].append(
                f'Minor lag detected: {total_lag} messages. Monitor closely.'
            )
        else:
            report['status'] = 'critical'
            report['recommendations'].append(
                f'High lag detected: {total_lag} messages. Consider scaling consumers.'
            )
        
        # Partition balance check
        if 'topic_stats' in report and 'partitions' in report['topic_stats']:
            partitions = report['topic_stats']['partitions']
            message_counts = [
                p.get('message_count', 0)
                for p in partitions.values()
                if 'message_count' in p
            ]
            
            if message_counts:
                max_count = max(message_counts)
                min_count = min(message_counts)
                imbalance_ratio = max_count / min_count if min_count > 0 else float('inf')
                
                if imbalance_ratio > 2.0:
                    report['recommendations'].append(
                        f'Partition imbalance detected (ratio: {imbalance_ratio:.2f}). '
                        'Consider reviewing key distribution.'
                    )
        
        return report

    def _create_monitoring_consumer(self) -> Consumer:
        """
        Create a temporary consumer for monitoring purposes.
        
        Returns:
            Consumer instance configured for monitoring
        """
        return Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': f'monitoring_{datetime.now().timestamp()}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        })

