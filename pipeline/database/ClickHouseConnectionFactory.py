"""
ClickHouse Connection Factory for managing database connections.
Provides abstraction for connection creation and lifecycle management.
"""
import logging
from typing import Any, Optional
from contextlib import contextmanager
import json

                
from pipeline.core.exceptions import ClickHouseConnectionError
from pipeline.compat.airflow_compat import get_connection 
from pipeline.interfaces.SQLConnectionFactory import SQLConnectionFactory

class ClickHouseConnectionFactory(SQLConnectionFactory):
    """
    Factory class for creating and managing ClickHouse connections.
    Implements Dependency Inversion Principle by providing abstraction layer.
    """

    def __init__(self, conn_id: str) -> None:
        """
        Initialize ClickHouse connection factory.

        Args:
            conn_id: Airflow connection ID or connection string
            is_connection_string: If True, conn_id is treated as a connection string
        """
        if not conn_id:
            raise ValueError("conn_id cannot be empty.")
        self.conn_id = conn_id
        self.logger = logging.getLogger(self.__class__.__name__)

    @contextmanager
    def get_connection(self):
        """
        Context manager for ClickHouse connections.
        Ensures proper connection cleanup.

        Yields:
            ClickHouse Client connection object

        Example:
            with factory.get_connection() as conn:
                conn.execute("SELECT 1")
        """
        from clickhouse_driver import Client # type: ignore
        
        client = None
        try:
            conn = get_connection(self.conn_id)
            extra = json.loads(conn.extra or "{}")

            client = Client(
                host=conn.host or 'localhost',
                port=conn.port or 9000,
                database=extra.get('database', conn.schema or 'default'),
                user=conn.login or 'default',
                password=conn.password or ''
            )
                
            yield client
            
        except ClickHouseConnectionError:
            raise
        except Exception as e:
            self.logger.error(
                f"[ClickHouseConnectionFactory.get_connection] ClickHouse connection failed | "
                f"conn_id={self.conn_id} | error={str(e)}",
                exc_info=True
            )
            raise ClickHouseConnectionError(
                f"Failed to connect to ClickHouse: {str(e)}"
            ) from e
        finally:
            if client:
                try:
                    client.disconnect()
                except Exception as close_error:
                    self.logger.warning(
                        f"[ClickHouseConnectionFactory.get_connection] Connection close failed | "
                        f"conn_id={self.conn_id} | error={str(close_error)}"
                    )

    def test_connection(self) -> bool:
        """
        Test ClickHouse connection.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self.get_connection() as client:
                client.execute("SELECT 1 AS Test")
            return True
        except Exception:
            return False

    def execute_query(self, query: str, params: Optional[dict] = None) -> Any:
        """
        Execute a query and return raw row tuples.

        Args:
            query: SQL query to execute
            params: Query parameters for parameterized queries

        Returns:
            Query results (list of tuples)
        """
        try:
            with self.get_connection() as client:
                return client.execute(query, params or {})
        except Exception as e:
            self.logger.error(f"[ClickHouseConnectionFactory.execute_query] Query execution failed: {e}")
            raise

    def execute_query_as_dicts(
        self,
        query: str,
        params: Optional[dict] = None,
    ) -> list:
        """
        Execute a SELECT and return rows as list of dictionaries.

        Args:
            query: SQL query to execute
            params: Named query parameters (clickhouse_driver ``%(name)s`` style)

        Returns:
            List of row dicts keyed by column name
        """
        try:
            with self.get_connection() as client:
                result = client.execute(
                    query,
                    params or {},
                    with_column_types=True,
                )
                if not result:
                    return []
                rows, columns_with_types = result
                if not rows:
                    return []
                column_names = [col[0] for col in columns_with_types]
                return [dict(zip(column_names, row)) for row in rows]
        except Exception as e:
            self.logger.error(f"[ClickHouseConnectionFactory.execute_query_as_dicts] Query (as dicts) execution failed: {e}")
            raise

    def execute_scalar(self, query: str, params: Optional[dict] = None) -> Any:
        """
        Execute a query and return the first cell of the first row.

        Args:
            query: SQL query expected to return a single scalar
            params: Named query parameters

        Returns:
            Scalar value or None if empty
        """
        try:
            with self.get_connection() as client:
                result = client.execute(query, params or {})
                if not result:
                    return None
                return result[0][0]
        except Exception as e:
            self.logger.error(f"[ClickHouseConnectionFactory.execute_scalar] Scalar query execution failed: {e}")
            raise

    def close_client(self) -> None:
        """Close client connection."""
        # client = self.get_connection()
        # if client is not None:
        #     client.disconnect()
            
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close_client()
        return False