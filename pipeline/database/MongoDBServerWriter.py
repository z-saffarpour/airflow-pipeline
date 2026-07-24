"""
MongoDB Writer with batch upsert via bulk_write.
Designed for Airflow ETL pipelines (MSSQL → MongoDB sync and similar).

Convention (mirrors MySQL writer API):
  - schema  → MongoDB database name
  - table   → MongoDB collection name
"""
import hashlib
import json
import logging
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from pipeline.core.exceptions import MongoDBQueryError
from pipeline.database.MongoDBConnectionFactory import MongoDBConnectionFactory
from pipeline.interfaces.DataWriter import DataWriter
from pipeline.utils.IdentifierValidator import IdentifierValidator


class MongoDBServerWriter(DataWriter):
    """
    MongoDB writer with batch upsert via pymongo bulk_write.

    Features:
    - Upsert by primary key filter (UpdateOne / ReplaceOne with upsert=True)
    - Optional mapping of a source id field to MongoDB ``_id``
    - Scoped delete_missing via a temporary keys staging collection
    - Optional hash-based change detection (skip unchanged docs)
    """

    def __init__(
        self,
        conn_id: str,
        id_as_mongo_id: Optional[str] = "id",
    ) -> None:
        self.conn_id = conn_id
        self.id_as_mongo_id = id_as_mongo_id
        self.connection_factory = MongoDBConnectionFactory(conn_id)
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _validate_name(name: str, label: str = "identifier") -> str:
        IdentifierValidator.validate_and_raise(name, label)
        return name

    def _keys_staging_collection_name(self, table: str, suffix: str) -> str:
        safe_suffix = re.sub(r"[^\w]", "_", str(suffix))[:50]
        return f"{table}_sync_keys_{safe_suffix}"

    def _normalize_value(self, value: Any) -> Any:
        """Convert MSSQL/Python values to BSON-friendly types."""
        if value is None:
            return None
        if isinstance(value, Decimal):
            try:
                from bson.decimal128 import Decimal128  # type: ignore

                return Decimal128(str(value))
            except Exception:
                return float(value)
        if isinstance(value, datetime):
            return value
        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time())
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        if isinstance(value, dict):
            return {k: self._normalize_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._normalize_value(v) for v in value]
        return value

    def _prepare_document(
        self,
        row: Dict[str, Any],
        key_columns: List[str],
    ) -> Dict[str, Any]:
        doc = {k: self._normalize_value(v) for k, v in row.items()}

        if self.id_as_mongo_id and self.id_as_mongo_id in doc:
            if "_id" not in doc:
                doc["_id"] = doc.pop(self.id_as_mongo_id)
            elif self.id_as_mongo_id != "_id":
                # Prefer explicit _id; drop the alias field to avoid duplication.
                doc.pop(self.id_as_mongo_id, None)

        # Ensure all key columns exist after id remapping.
        for col in key_columns:
            mongo_col = "_id" if col == self.id_as_mongo_id else col
            if mongo_col not in doc and col in row:
                doc[mongo_col] = self._normalize_value(row[col])

        return doc

    def _mongo_key_columns(self, key_columns: List[str]) -> List[str]:
        """Map logical key columns to Mongo field names (id → _id when configured)."""
        result: List[str] = []
        for col in key_columns:
            if self.id_as_mongo_id and col == self.id_as_mongo_id:
                result.append("_id")
            else:
                result.append(col)
        return result

    def _build_key_filter(
        self,
        doc: Dict[str, Any],
        mongo_key_columns: List[str],
    ) -> Dict[str, Any]:
        filt: Dict[str, Any] = {}
        for col in mongo_key_columns:
            if col not in doc:
                raise KeyError(f"Key column '{col}' missing from document")
            filt[col] = doc[col]
        return filt

    @staticmethod
    def _doc_content_hash(doc: Dict[str, Any], exclude_keys: Optional[List[str]] = None) -> str:
        exclude = set(exclude_keys or [])
        payload = {
            k: doc[k]
            for k in sorted(doc.keys())
            if k not in exclude
        }
        raw = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def insert_batch(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        if not data:
            return 0
        self._validate_name(schema, "database name")
        self._validate_name(table, "collection name")

        from pymongo import InsertOne  # type: ignore

        total = 0
        with self.connection_factory.get_collection(table, database=schema) as coll:
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                ops = [
                    InsertOne(self._prepare_document(row, key_columns=[]))
                    for row in batch
                ]
                result = coll.bulk_write(ops, ordered=False)
                total += result.inserted_count
        return total

    def update_batch(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        if not data:
            return 0
        self._validate_name(schema, "database name")
        self._validate_name(table, "collection name")
        for col in key_columns:
            self._validate_name(col, "column name")

        from pymongo import ReplaceOne  # type: ignore

        mongo_keys = self._mongo_key_columns(key_columns)
        total = 0
        with self.connection_factory.get_collection(table, database=schema) as coll:
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                ops = []
                for row in batch:
                    doc = self._prepare_document(row, key_columns)
                    filt = self._build_key_filter(doc, mongo_keys)
                    ops.append(ReplaceOne(filt, doc, upsert=False))
                if ops:
                    result = coll.bulk_write(ops, ordered=False)
                    total += result.modified_count
        return total

    def delete_batch(
        self,
        schema: str,
        table: str,
        key_column: str,
        key_values: List[Any],
        batch_size: int = 1000,
        staging_schema: Optional[str] = None,
    ) -> int:
        if not key_values:
            return 0
        self._validate_name(schema, "database name")
        self._validate_name(table, "collection name")
        self._validate_name(key_column, "column name")

        mongo_key = (
            "_id"
            if self.id_as_mongo_id and key_column == self.id_as_mongo_id
            else key_column
        )
        total = 0
        with self.connection_factory.get_collection(table, database=schema) as coll:
            for i in range(0, len(key_values), batch_size):
                batch = key_values[i : i + batch_size]
                normalized = [self._normalize_value(v) for v in batch]
                result = coll.delete_many({mongo_key: {"$in": normalized}})
                total += result.deleted_count
        return total

    def prepare_keys_staging_table(
        self,
        schema: str,
        table: str,
        key_columns: List[str],
        suffix: str,
        staging_schema: Optional[str] = None,
    ) -> str:
        """
        Create (or recreate) a temporary keys staging collection.

        Returns a fully-qualified name ``database.collection`` used by
        append / delete / drop helpers.
        """
        self._validate_name(schema, "database name")
        self._validate_name(table, "collection name")
        for col in key_columns:
            self._validate_name(col, "column name")
        self._validate_name(suffix, "staging suffix")

        staging_db = staging_schema or schema
        self._validate_name(staging_db, "staging database name")
        coll_name = self._keys_staging_collection_name(table, suffix)

        with self.connection_factory.get_database(database=staging_db) as db:
            db.drop_collection(coll_name)
            staging = db[coll_name]
            # Create empty collection + unique index on mongo key fields.
            mongo_keys = self._mongo_key_columns(key_columns)
            index_spec = [(k, 1) for k in mongo_keys]
            staging.create_index(index_spec, unique=True, name="sync_keys_pk")

        qualified = f"{staging_db}.{coll_name}"
        self.logger.info(
            "[MongoDBServerWriter.prepare_keys_staging_table] Prepared %s for %s.%s",
            qualified,
            schema,
            table,
        )
        return qualified

    @staticmethod
    def _split_qualified_collection(qualified: str) -> Tuple[str, str]:
        if "." not in qualified:
            raise ValueError(
                f"Expected database.collection staging name, got: {qualified}"
            )
        database, collection = qualified.split(".", 1)
        return database, collection

    def append_keys_to_staging(
        self,
        keys_staging_table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000,
    ) -> int:
        if not data:
            return 0

        for col in key_columns:
            self._validate_name(col, "column name")

        staging_db, staging_coll = self._split_qualified_collection(keys_staging_table)
        mongo_keys = self._mongo_key_columns(key_columns)
        total = 0

        with self.connection_factory.get_collection(
            staging_coll, database=staging_db
        ) as coll:
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                docs = []
                for row in batch:
                    prepared = self._prepare_document(row, key_columns)
                    key_doc = {k: prepared[k] for k in mongo_keys}
                    docs.append(key_doc)
                if docs:
                    # ordered=False + ignore duplicate key errors for re-runs
                    try:
                        result = coll.insert_many(docs, ordered=False)
                        total += len(result.inserted_ids)
                    except Exception as exc:
                        # DuplicateKeyError still inserts non-dup docs when ordered=False
                        details = getattr(exc, "details", None) or {}
                        n = details.get("nInserted")
                        if n is not None:
                            total += int(n)
                        else:
                            raise MongoDBQueryError(
                                f"append_keys_to_staging failed: {exc}"
                            ) from exc
        return total

    def delete_missing_in_scope(
        self,
        schema: str,
        table: str,
        key_columns: List[str],
        keys_staging_table: str,
        scope_column: str,
        min_key: Any,
        max_key: Any,
    ) -> int:
        self._validate_name(schema, "database name")
        self._validate_name(table, "collection name")
        for col in key_columns:
            self._validate_name(col, "column name")
        self._validate_name(scope_column, "column name")

        if min_key is None or max_key is None:
            self.logger.warning(
                "[MongoDBServerWriter.delete_missing_in_scope] Skipping scoped delete "
                "for %s.%s; min_key or max_key is NULL",
                schema,
                table,
            )
            return 0

        staging_db, staging_coll = self._split_qualified_collection(keys_staging_table)
        mongo_keys = self._mongo_key_columns(key_columns)
        mongo_scope = (
            "_id"
            if self.id_as_mongo_id and scope_column == self.id_as_mongo_id
            else scope_column
        )
        scope_min = self._normalize_value(min_key)
        scope_max = self._normalize_value(max_key)

        with self.connection_factory.get_client() as client:
            staging = client[staging_db][staging_coll]
            target = client[schema][table]

            # Load all staged keys into a set of tuples for membership checks.
            staged_keys = set()
            for doc in staging.find({}, {k: 1 for k in mongo_keys}):
                staged_keys.add(tuple(doc.get(k) for k in mongo_keys))

            scope_filter = {mongo_scope: {"$gte": scope_min, "$lte": scope_max}}
            to_delete_ids = []
            projection = {k: 1 for k in mongo_keys}
            for doc in target.find(scope_filter, projection):
                key_tuple = tuple(doc.get(k) for k in mongo_keys)
                if key_tuple not in staged_keys:
                    to_delete_ids.append(doc["_id"])

            deleted = 0
            batch_size = 1000
            for i in range(0, len(to_delete_ids), batch_size):
                batch = to_delete_ids[i : i + batch_size]
                result = target.delete_many({"_id": {"$in": batch}})
                deleted += result.deleted_count

            self.logger.info(
                "[MongoDBServerWriter.delete_missing_in_scope] Scoped delete for %s.%s | "
                "scope=%s | min=%s | max=%s | deleted=%d",
                schema,
                table,
                mongo_scope,
                scope_min,
                scope_max,
                deleted,
            )
            return deleted

    def drop_keys_staging_table(self, keys_staging_table: str) -> None:
        staging_db, staging_coll = self._split_qualified_collection(keys_staging_table)
        with self.connection_factory.get_database(database=staging_db) as db:
            db.drop_collection(staging_coll)
            self.logger.info(
                "[MongoDBServerWriter.drop_keys_staging_table] Dropped %s",
                keys_staging_table,
            )

    def upsert_batch(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000,
        delete_missing: bool = False,
        unique_keys: Optional[Tuple[Tuple[str, ...], ...]] = None,
        resolve_unique_key_conflicts: bool = False,
        use_hash_change_detection: bool = True,
        staging_schema: Optional[str] = None,
        staging_suffix: str = "upsert",
    ) -> Dict[str, int]:
        if not data:
            self.logger.warning("[MongoDBServerWriter.upsert_batch] No data to upsert")
            return {"inserted": 0, "updated": 0, "deleted": 0}

        self._validate_name(schema, "database name")
        self._validate_name(table, "collection name")
        for col in key_columns:
            self._validate_name(col, "column name")

        if unique_keys or resolve_unique_key_conflicts:
            self.logger.debug(
                "[MongoDBServerWriter.upsert_batch] unique_keys / conflict resolution "
                "are not applied for MongoDB (API accepted for orchestrator parity)"
            )

        from pymongo import ReplaceOne  # type: ignore

        mongo_keys = self._mongo_key_columns(key_columns)
        inserted = 0
        updated = 0

        with self.connection_factory.get_collection(table, database=schema) as coll:
            for i in range(0, len(data), batch_size):
                batch = data[i : i + batch_size]
                ops = []

                if use_hash_change_detection:
                    # Prefetch existing docs for this batch to skip unchanged rows.
                    filters = []
                    prepared_docs = []
                    for row in batch:
                        doc = self._prepare_document(row, key_columns)
                        prepared_docs.append(doc)
                        filters.append(self._build_key_filter(doc, mongo_keys))

                    existing_by_key: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
                    if filters:
                        # $or of key filters; for single-key (_id) use $in instead.
                        if len(mongo_keys) == 1:
                            key_name = mongo_keys[0]
                            values = [f[key_name] for f in filters]
                            for existing in coll.find({key_name: {"$in": values}}):
                                existing_by_key[(existing.get(key_name),)] = existing
                        else:
                            for existing in coll.find({"$or": filters}):
                                kt = tuple(existing.get(k) for k in mongo_keys)
                                existing_by_key[kt] = existing

                    for doc in prepared_docs:
                        filt = self._build_key_filter(doc, mongo_keys)
                        kt = tuple(doc.get(k) for k in mongo_keys)
                        existing = existing_by_key.get(kt)
                        if existing is not None:
                            new_hash = self._doc_content_hash(doc)
                            old_hash = self._doc_content_hash(existing)
                            if new_hash == old_hash:
                                continue
                            ops.append(ReplaceOne(filt, doc, upsert=False))
                        else:
                            ops.append(ReplaceOne(filt, doc, upsert=True))
                else:
                    for row in batch:
                        doc = self._prepare_document(row, key_columns)
                        filt = self._build_key_filter(doc, mongo_keys)
                        ops.append(ReplaceOne(filt, doc, upsert=True))

                if not ops:
                    continue

                result = coll.bulk_write(ops, ordered=False)
                inserted += result.upserted_count
                updated += result.modified_count

        deleted = 0
        if delete_missing:
            self.logger.warning(
                "[MongoDBServerWriter.upsert_batch] delete_missing=True on upsert_batch "
                "is ignored; use prepare_keys_staging_table + delete_missing_in_scope"
            )

        self.logger.info(
            "[MongoDBServerWriter.upsert_batch] %s.%s sync completed | "
            "inserted=%s | updated=%s | hash_change_detection=%s",
            schema,
            table,
            inserted,
            updated,
            use_hash_change_detection,
        )
        return {
            "success": True,
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "total": inserted + updated,
        }
