"""
MongoDB Connection Factory for managing database connections.
Provides abstraction for connection creation and lifecycle management.
"""
import json
import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from pipeline.compat.airflow_compat import get_connection
from pipeline.core.exceptions import (
    MongoDBConnectionError,
    MongoDBQueryError,
)
from pipeline.interfaces.ConnectionFactory import ConnectionFactory


class MongoDBConnectionFactory(ConnectionFactory):
    """
    Factory for creating and managing MongoDB clients via Airflow Connection + pymongo.
    """

    def __init__(self, conn_id: str, database: Optional[str] = None) -> None:
        if not conn_id:
            raise ValueError("conn_id cannot be empty.")
        self.conn_id = conn_id
        self.database_override = database
        self.logger = logging.getLogger(self.__class__.__name__)

    def _parse_extra(self, conn) -> Dict[str, Any]:
        raw = getattr(conn, "extra", None) or "{}"
        if isinstance(raw, dict):
            return raw
        try:
            return json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            self.logger.warning(
                "[MongoDBConnectionFactory] Invalid extra JSON for conn_id=%s",
                self.conn_id,
            )
            return {}

    def _resolve_database(self, conn, extra: Dict[str, Any]) -> str:
        if self.database_override:
            return self.database_override
        if extra.get("database"):
            return str(extra["database"])
        if getattr(conn, "schema", None):
            return str(conn.schema)
        return "admin"

    def _build_uri(self, conn, extra: Dict[str, Any]) -> str:
        if extra.get("uri"):
            return str(extra["uri"])

        host = conn.host or "localhost"
        port = conn.port or 27017
        user = conn.login
        password = conn.password
        auth_source = extra.get("authSource") or extra.get("auth_source")

        if user:
            user_enc = quote_plus(str(user))
            pass_enc = quote_plus(str(password or ""))
            auth = f"{user_enc}:{pass_enc}@"
        else:
            auth = ""

        uri = f"mongodb://{auth}{host}:{port}"
        query_parts: List[str] = []
        if auth_source:
            query_parts.append(f"authSource={quote_plus(str(auth_source))}")
        if extra.get("replicaSet"):
            query_parts.append(f"replicaSet={quote_plus(str(extra['replicaSet']))}")
        if extra.get("tls") or extra.get("ssl"):
            query_parts.append("tls=true")
        if query_parts:
            uri = f"{uri}/?{'&'.join(query_parts)}"
        return uri

    def _client_kwargs(self, extra: Dict[str, Any]) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        if "serverSelectionTimeoutMS" in extra:
            kwargs["serverSelectionTimeoutMS"] = int(extra["serverSelectionTimeoutMS"])
        else:
            kwargs["serverSelectionTimeoutMS"] = 10000
        if "connectTimeoutMS" in extra:
            kwargs["connectTimeoutMS"] = int(extra["connectTimeoutMS"])
        return kwargs

    @contextmanager
    def get_client(self):
        """Context manager yielding a pymongo MongoClient."""
        from pymongo import MongoClient  # type: ignore

        client = None
        try:
            conn = get_connection(self.conn_id)
            extra = self._parse_extra(conn)
            uri = self._build_uri(conn, extra)
            self.logger.info(
                "[MongoDBConnectionFactory.get_client] Opening MongoDB client | conn_id='%s'",
                self.conn_id,
            )
            client = MongoClient(uri, **self._client_kwargs(extra))
            # Force early failure if the cluster is unreachable.
            client.admin.command("ping")
            yield client
        except MongoDBConnectionError:
            raise
        except Exception as exc:
            self.logger.error(
                "[MongoDBConnectionFactory.get_client] Connection failed | "
                "conn_id='%s' | error=%s",
                self.conn_id,
                exc,
                exc_info=True,
            )
            raise MongoDBConnectionError(
                f"Failed to connect to MongoDB: {exc}"
            ) from exc
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    self.logger.error(
                        "[MongoDBConnectionFactory.get_client] Failed to close client.",
                        exc_info=True,
                    )

    @contextmanager
    def get_database(self, database: Optional[str] = None):
        """Context manager yielding a pymongo Database."""
        with self.get_client() as client:
            conn = get_connection(self.conn_id)
            extra = self._parse_extra(conn)
            db_name = database or self._resolve_database(conn, extra)
            yield client[db_name]

    @contextmanager
    def get_collection(self, collection: str, database: Optional[str] = None):
        """Context manager yielding a pymongo Collection."""
        if not collection:
            raise ValueError("collection cannot be empty.")
        with self.get_database(database=database) as db:
            yield db[collection]

    def test_connection(self) -> bool:
        """Return True if ping succeeds."""
        try:
            with self.get_client() as client:
                client.admin.command("ping")
            return True
        except Exception:
            return False

    def count_documents(
        self,
        collection: str,
        filter_query: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> int:
        """Count documents matching filter."""
        try:
            with self.get_collection(collection, database=database) as coll:
                return int(coll.count_documents(filter_query or {}))
        except Exception as exc:
            self.logger.error(
                "[MongoDBConnectionFactory.count_documents] Failed: %s",
                exc,
                exc_info=True,
            )
            raise MongoDBQueryError(f"MongoDB count failed: {exc}") from exc

    def find_documents(
        self,
        collection: str,
        filter_query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[List] = None,
        database: Optional[str] = None,
        batch_size: int = 10000,
        skip: int = 0,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Execute find and return all matching documents (use sparingly)."""
        try:
            with self.get_collection(collection, database=database) as coll:
                cursor = coll.find(filter_query or {}, projection)
                if sort:
                    cursor = cursor.sort(list(sort))
                if skip:
                    cursor = cursor.skip(int(skip))
                if limit is not None:
                    cursor = cursor.limit(int(limit))
                cursor = cursor.batch_size(int(batch_size))
                return list(cursor)
        except Exception as exc:
            self.logger.error(
                "[MongoDBConnectionFactory.find_documents] Failed: %s",
                exc,
                exc_info=True,
            )
            raise MongoDBQueryError(f"MongoDB find failed: {exc}") from exc

    def aggregate(
        self,
        collection: str,
        pipeline: List[Dict[str, Any]],
        database: Optional[str] = None,
        batch_size: int = 10000,
    ) -> List[Dict[str, Any]]:
        """Run an aggregation pipeline and return all results."""
        try:
            with self.get_collection(collection, database=database) as coll:
                cursor = coll.aggregate(pipeline, batchSize=int(batch_size))
                return list(cursor)
        except Exception as exc:
            self.logger.error(
                "[MongoDBConnectionFactory.aggregate] Failed: %s",
                exc,
                exc_info=True,
            )
            raise MongoDBQueryError(f"MongoDB aggregate failed: {exc}") from exc
