"""
MongoDB Data Reader — streams collection / aggregation results in batches.
"""
import json
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple

from airflow.exceptions import AirflowException  # type: ignore

from pipeline.core.exceptions import DataReadError, MongoDBQueryError
from pipeline.database.MongoDBConnectionFactory import MongoDBConnectionFactory
from pipeline.interfaces.DataReader import DataReader


class MongoDBDataReader(DataReader):
    """
    Reads documents from MongoDB in streaming mode using Generator.
    Documents are normalized to flat dicts suitable for MSSQL upsert.
    """

    def __init__(
        self,
        conn_id: str,
        batch_size: int = 10000,
        database: Optional[str] = None,
        rename_id_to: Optional[str] = "id",
        serialize_nested: bool = True,
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conn_id = conn_id
        self.batch_size = batch_size
        self.database = database
        self.rename_id_to = rename_id_to
        self.serialize_nested = serialize_nested
        self.connection_factory = MongoDBConnectionFactory(
            conn_id=conn_id,
            database=database,
        )

    def _normalize_value(self, value: Any) -> Any:
        try:
            from bson import ObjectId  # type: ignore
            from bson.decimal128 import Decimal128  # type: ignore
            from bson.binary import Binary  # type: ignore
        except ImportError:
            ObjectId = ()  # type: ignore
            Decimal128 = ()  # type: ignore
            Binary = ()  # type: ignore

        if ObjectId and isinstance(value, ObjectId):
            return str(value)
        if Decimal128 and isinstance(value, Decimal128):
            return Decimal(str(value.to_decimal()))
        if Binary and isinstance(value, Binary):
            return bytes(value)
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, Decimal):
            return value
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        if isinstance(value, dict):
            if self.serialize_nested:
                return json.dumps(
                    {k: self._normalize_value(v) for k, v in value.items()},
                    default=str,
                )
            return {k: self._normalize_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            if self.serialize_nested:
                return json.dumps(
                    [self._normalize_value(v) for v in value],
                    default=str,
                )
            return [self._normalize_value(v) for v in value]
        return value

    def normalize_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a BSON document to an MSSQL-friendly dict."""
        if not isinstance(document, dict):
            raise TypeError(f"Expected dict document, got {type(document).__name__}")

        normalized: Dict[str, Any] = {}
        for key, value in document.items():
            out_key = key
            if key == "_id" and self.rename_id_to:
                out_key = self.rename_id_to
            normalized[out_key] = self._normalize_value(value)
        return normalized

    def normalize_batch(self, documents: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.normalize_document(doc) for doc in documents]

    def get_total_count(
        self,
        table_name: Optional[str] = None,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = "int",
        custom_query: Optional[Dict[str, Any]] = None,
        params: Optional[Tuple] = None,
        collection: Optional[str] = None,
        filter_query: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> int:
        """
        Count documents in a collection.

        ``table_name`` / ``collection`` are aliases for the Mongo collection name.
        ``custom_query`` may be a filter dict when provided.
        """
        coll = collection or table_name
        if not coll:
            raise ValueError("Either 'collection' or 'table_name' must be provided.")

        mongo_filter = filter_query
        if mongo_filter is None and isinstance(custom_query, dict):
            mongo_filter = custom_query
        if mongo_filter is None:
            mongo_filter = {}

        # Optional date filter for interface parity with SQL readers.
        if date_column and date_key is not None:
            if date_column_type == "int":
                mongo_filter = {**mongo_filter, date_column: int(date_key)}
            else:
                mongo_filter = {**mongo_filter, date_column: date_key}

        self.logger.info(
            f"[MongoDBDataReader.get_total_count] START | collection={coll} | database={database or self.database}"
        )
        try:
            count = self.connection_factory.count_documents(
                collection=coll,
                filter_query=mongo_filter,
                database=database or self.database,
            )
            self.logger.info(f"[MongoDBDataReader.get_total_count] SUCCESS | count={count}")
            return count
        except MongoDBQueryError:
            raise
        except Exception as exc:
            self.logger.error(f"[MongoDBDataReader.get_total_count] Unexpected ERROR | error={exc}", exc_info=True)
            raise DataReadError(f"Failed to get total count: {exc}") from exc

    def stream_data(
        self,
        table_name: str,
        order_by_column: str,
        columns: list = None,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = "int",
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Stream collection documents with keyset pagination on ``order_by_column``.
        """
        filter_query: Dict[str, Any] = {}
        if date_column and date_key is not None:
            if date_column_type == "int":
                filter_query[date_column] = int(date_key)
            else:
                filter_query[date_column] = date_key

        projection = None
        if columns:
            projection = {col: 1 for col in columns}
            if order_by_column not in projection:
                projection[order_by_column] = 1
            if "_id" not in projection and (
                self.rename_id_to is None or self.rename_id_to in (columns or [])
            ):
                projection["_id"] = 1

        yield from self.stream_collection(
            collection=table_name,
            filter_query=filter_query,
            projection=projection,
            sort=((order_by_column, 1),),
            chunk_column=order_by_column,
        )

    def stream_query(
        self,
        query: str,
        count_query: str = None,
        parameters: Optional[tuple] = None,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Compatibility shim: ``query`` must be a JSON object describing the find.

        Expected JSON keys:
            collection (required), database, filter, projection, sort
        """
        try:
            payload = json.loads(query) if isinstance(query, str) else query
        except json.JSONDecodeError as exc:
            raise AirflowException(
                "MongoDBDataReader.stream_query expects JSON describing the find"
            ) from exc

        if not isinstance(payload, dict) or "collection" not in payload:
            raise AirflowException(
                "MongoDB stream_query JSON must include 'collection'"
            )

        sort = payload.get("sort")
        sort_tuple = tuple(tuple(item) for item in sort) if sort else None

        yield from self.stream_collection(
            collection=payload["collection"],
            filter_query=payload.get("filter") or payload.get("filter_query"),
            projection=payload.get("projection"),
            sort=sort_tuple,
            database=payload.get("database"),
            aggregation_pipeline=payload.get("aggregation_pipeline")
            or payload.get("pipeline"),
        )

    def stream_collection(
        self,
        collection: str,
        filter_query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, Any]] = None,
        sort: Optional[Sequence[Tuple[str, int]]] = None,
        database: Optional[str] = None,
        aggregation_pipeline: Optional[Sequence[Dict[str, Any]]] = None,
        range_column: Optional[str] = None,
        min_key: Any = None,
        max_key: Any = None,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """Stream find or aggregate results in batches."""
        processed = 0
        batch_number = 0
        db_name = database or self.database
        mongo_filter = dict(filter_query or {})

        if range_column is not None and min_key is not None and max_key is not None:
            mongo_filter[range_column] = {"$gte": min_key, "$lte": max_key}

        self.logger.info(
            f"[MongoDBDataReader.stream_collection] START | collection={collection} | database={db_name} | "
            f"batch_size={self.batch_size} | has_pipeline={aggregation_pipeline is not None}"
        )

        try:
            with self.connection_factory.get_collection(
                collection,
                database=db_name,
            ) as coll:
                if aggregation_pipeline is not None:
                    pipeline = list(aggregation_pipeline)
                    if mongo_filter:
                        pipeline = [{"$match": mongo_filter}, *pipeline]
                    cursor = coll.aggregate(pipeline, batchSize=int(self.batch_size))
                else:
                    cursor = coll.find(mongo_filter, projection)
                    if sort:
                        cursor = cursor.sort(list(sort))
                    cursor = cursor.batch_size(int(self.batch_size))

                batch: List[Dict[str, Any]] = []
                for document in cursor:
                    batch.append(document)
                    if len(batch) >= self.batch_size:
                        batch_number += 1
                        processed += len(batch)
                        self.logger.info(
                            '[MongoDBDataReader.stream_collection] Batch %s | rows=%s | processed=%s',
                            batch_number,
                            len(batch),
                            format(processed, ','),
                        )
                        yield self.normalize_batch(batch)
                        batch = []

                if batch:
                    batch_number += 1
                    processed += len(batch)
                    self.logger.info(
                        f"[MongoDBDataReader.stream_collection] Batch {batch_number} | rows={len(batch)} | processed={processed:,}"
                    )
                    yield self.normalize_batch(batch)
        except Exception as exc:
            self.logger.error(
                f"[MongoDBDataReader.stream_collection] ERROR | processed={processed} | error={exc}",
                exc_info=True
            )
            raise AirflowException(f"Streaming MongoDB collection failed: {exc}") from exc

        self.logger.info(
            f"[MongoDBDataReader.stream_collection] FINISH | processed={processed} | batches={batch_number}"
        )

    def get_field_bounds(
        self,
        collection: str,
        field: str,
        filter_query: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> Tuple[Any, Any]:
        """Return (min, max) for a field using aggregation."""
        pipeline = [
            {"$match": filter_query or {}},
            {
                "$group": {
                    "_id": None,
                    "min_key": {"$min": f"${field}"},
                    "max_key": {"$max": f"${field}"},
                }
            },
        ]
        rows = self.connection_factory.aggregate(
            collection=collection,
            pipeline=pipeline,
            database=database or self.database,
            batch_size=1,
        )
        if not rows:
            return None, None
        row = rows[0]
        return row.get("min_key"), row.get("max_key")

    def plan_bucket_chunks(
        self,
        collection: str,
        chunk_column: str,
        chunk_count: int,
        filter_query: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Plan key ranges via ``$bucketAuto`` (MongoDB 3.4+).
        Returns list of {chunk_no, min_key, max_key, row_count, chunk_column}.
        """
        if chunk_count < 1:
            raise ValueError("chunk_count must be >= 1")

        pipeline = [
            {"$match": filter_query or {}},
            {
                "$bucketAuto": {
                    "groupBy": f"${chunk_column}",
                    "buckets": int(chunk_count),
                    "output": {"row_count": {"$sum": 1}},
                }
            },
        ]
        rows = self.connection_factory.aggregate(
            collection=collection,
            pipeline=pipeline,
            database=database or self.database,
            batch_size=self.batch_size,
        )

        chunks: List[Dict[str, Any]] = []
        for index, row in enumerate(rows, start=1):
            bounds = row.get("_id") or {}
            chunks.append(
                {
                    "chunk_no": index,
                    "min_key": bounds.get("min"),
                    "max_key": bounds.get("max"),
                    "row_count": int(row.get("row_count") or 0),
                    "chunk_column": chunk_column,
                }
            )
        return chunks
