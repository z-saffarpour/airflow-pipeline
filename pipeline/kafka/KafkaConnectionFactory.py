"""
Kafka Connection Factory for managing cluster clients.
Provides abstraction for broker resolution and client lifecycle management.
"""
import logging
from contextlib import contextmanager
from typing import Any, Dict, Optional

from confluent_kafka.admin import AdminClient  # type: ignore

from pipeline.core.exceptions import KafkaConnectionError
from pipeline.utils.connection_utils import get_connection, load_connection_extra


class KafkaConnectionFactory:
    """
    Factory class for creating and managing Kafka clients from an Airflow Connection.

    Mirrors ConnectionFactory / ClickHouseConnectionFactory:
    - resolve credentials from Airflow Connection (conn_id)
    - build shared client config (bootstrap, SASL/SSL)
    - expose AdminClient + test_connection

    Supported connection extra fields:
    - bootstrap_servers  : override host:port with a full broker list
    - client_id          : default Kafka client identifier (default: "airflow")
    - security_protocol  : e.g. "SASL_SSL"
    - sasl_mechanism     : e.g. "PLAIN" or "SCRAM-SHA-256"
    - sasl_username      : SASL username
    - sasl_password      : SASL password
    """

    def __init__(self, conn_id: str) -> None:
        """
        Initialize Kafka connection factory.

        Args:
            conn_id: Airflow Connection ID for the Kafka cluster
        """
        if not conn_id:
            raise ValueError("conn_id cannot be empty.")
        self.conn_id = conn_id
        self.logger = logging.getLogger(self.__class__.__name__)

    def get_bootstrap_servers(self) -> str:
        """
        Resolve Kafka bootstrap servers from the Airflow Connection.

        Resolution order:
        1. extra.bootstrap_servers
        2. {host}:{port} (port defaults to 9092)
        """
        conn = get_connection(self.conn_id)
        extra = load_connection_extra(conn)
        return extra.get("bootstrap_servers") or f"{conn.host}:{conn.port or 9092}"

    def get_client_config(
        self,
        client_id: Optional[str] = None,
        **overrides: Any,
    ) -> Dict[str, Any]:
        """
        Build a confluent-kafka client config dict from the Airflow Connection.

        Args:
            client_id: Optional override for client.id
            **overrides: Extra confluent-kafka config keys to merge last

        Returns:
            Config dict suitable for AdminClient / Producer / Consumer
        """
        conn = get_connection(self.conn_id)
        extra = load_connection_extra(conn)

        config: Dict[str, Any] = {
            "bootstrap.servers": (
                extra.get("bootstrap_servers")
                or f"{conn.host}:{conn.port or 9092}"
            ),
            "client.id": client_id or extra.get("client_id", "airflow"),
        }

        if extra.get("security_protocol"):
            config["security.protocol"] = extra["security_protocol"]

        if extra.get("sasl_mechanism"):
            config["sasl.mechanism"] = extra["sasl_mechanism"]
            config["sasl.username"] = extra.get("sasl_username")
            config["sasl.password"] = extra.get("sasl_password")

        if overrides:
            config.update(overrides)

        return config

    def create_admin_client(self, **overrides: Any) -> AdminClient:
        """
        Create a confluent-kafka AdminClient for this connection.

        Args:
            **overrides: Optional config overrides passed to get_client_config

        Returns:
            Configured AdminClient instance

        Raises:
            KafkaConnectionError: If client creation fails
        """
        try:
            config = self.get_client_config(**overrides)
            self.logger.debug(
                "Creating Kafka AdminClient | conn_id=%s | bootstrap=%s",
                self.conn_id,
                config.get("bootstrap.servers"),
            )
            return AdminClient(config)
        except Exception as e:
            self.logger.error(
                "Failed to create Kafka AdminClient",
                extra={"error": str(e), "conn_id": self.conn_id},
                exc_info=True,
            )
            raise KafkaConnectionError(
                f"Failed to create Kafka AdminClient for {self.conn_id}: {e}"
            ) from e

    @contextmanager
    def get_admin_client(self, **overrides: Any):
        """
        Context manager for Kafka AdminClient (low-level escape hatch).

        Prefer list_topics() / get_cluster_info() for common operations.
        AdminClient has no explicit close; this mainly standardizes usage
        and error wrapping to match other connection factories.

        Yields:
            AdminClient
        """
        admin = None
        try:
            admin = self.create_admin_client(**overrides)
            yield admin
        except KafkaConnectionError:
            raise
        except Exception as e:
            self.logger.error(
                "Kafka AdminClient operation failed",
                extra={"error": str(e), "conn_id": self.conn_id},
                exc_info=True,
            )
            raise KafkaConnectionError(
                f"Failed to use Kafka AdminClient for {self.conn_id}: {e}"
            ) from e
        finally:
            # confluent AdminClient has no disconnect; drop reference for GC
            admin = None

    def list_topics(self, timeout: float = 10.0, **overrides: Any):
        """
        List cluster topics/brokers metadata via AdminClient.

        Args:
            timeout: Seconds to wait for metadata
            **overrides: Optional client config overrides

        Returns:
            confluent_kafka ClusterMetadata (brokers, topics, ...)

        Raises:
            KafkaConnectionError: If metadata cannot be fetched
        """
        try:
            with self.get_admin_client(**overrides) as admin:
                return admin.list_topics(timeout=timeout)
        except KafkaConnectionError:
            raise
        except Exception as e:
            self.logger.error(
                "Kafka list_topics failed",
                extra={"error": str(e), "conn_id": self.conn_id},
                exc_info=True,
            )
            raise KafkaConnectionError(
                f"Failed to list Kafka topics for {self.conn_id}: {e}"
            ) from e

    def get_cluster_info(self, timeout: float = 10.0) -> Dict[str, Any]:
        """
        Return a serializable summary of cluster health metadata.

        Returns:
            dict with broker_count, topic_count, bootstrap_servers
        """
        metadata = self.list_topics(timeout=timeout)
        return {
            "broker_count": len(metadata.brokers),
            "topic_count": len(metadata.topics),
            "bootstrap_servers": self.get_bootstrap_servers(),
        }

    def test_connection(self, timeout: float = 10.0) -> bool:
        """
        Test Kafka connectivity by listing cluster brokers.

        Returns:
            True if at least one broker is reachable, False otherwise
        """
        try:
            metadata = self.list_topics(timeout=timeout)
            return bool(metadata.brokers)
        except Exception:
            self.logger.exception(
                "Kafka connection test failed | conn_id=%s", self.conn_id
            )
            return False
