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

    @staticmethod
    def _validate_document_columns(data: List[Dict[str, Any]]) -> None:
        """
        Validate every data-column name that will become a MongoDB document
        field, not just the key columns used in filters.

        Previously only ``key_columns`` were run through
        ``IdentifierValidator``; a source column name flowed straight from
        ``row.items()`` (see ``_prepare_document``) into the document with no
        check at all. In practice the risk is low since documents are written
        via ``ReplaceOne``/``InsertOne`` (a full-document replace, not a
        ``$set`` built from field names), but an unchecked field name is
        still inconsistent with every other writer in this pipeline.

        Validates the union of keys across the whole batch once (documents
        aren't guaranteed to share identical keys) rather than per row, to
        avoid re-validating the same column names on every document.
        """
        columns = set()
        for row in data:
            columns.update(row.keys())
        for column in columns:
            IdentifierValidator.validate_and_raise(column, "column name")

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
        self._validate_document_columns(data)

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
        self._validate_document_columns(data)

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
        self._validate_document_columns(data)

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

    @staticmethod
    def _build_lookup_missing_pipeline(
        staging_coll: str,
        mongo_keys: List[str],
        scope_filter: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Build an aggregation pipeline that finds target documents (within
        ``scope_filter``) whose key is absent from the staging collection,
        entirely server-side - the Mongo equivalent of the
        ``DELETE ... WHERE NOT EXISTS (SELECT 1 FROM staging ...)`` pattern
        used by the SQL writers. Requires ``staging_coll`` to live in the
        same database as the aggregation runs against ($lookup does not
        support crossing databases on a standard deployment).

        A pipeline-style $lookup (via ``let``/``pipeline``) is used instead
        of the simpler localField/foreignField form so this works uniformly
        for both single-column and composite keys.
        """
        let_vars = {f"k{i}": f"${col}" for i, col in enumerate(mongo_keys)}
        match_exprs = [
            {"$eq": [f"$${name}", f"${col}"]}
            for name, col in zip(let_vars.keys(), mongo_keys)
        ]
        return [
            {"$match": scope_filter},
            {
                "$lookup": {
                    "from": staging_coll,
                    "let": let_vars,
                    "pipeline": [
                        {"$match": {"$expr": {"$and": match_exprs}}},
                        {"$limit": 1},
                    ],
                    "as": "_sync_match",
                }
            },
            {"$match": {"_sync_match": {"$size": 0}}},
            {"$project": {"_id": 1}},
        ]

    @staticmethod
    def _delete_by_id_in_batches(coll, ids: List[Any], batch_size: int = 1000) -> int:
        """Delete documents by ``_id`` in batches, without holding all ids at once."""
        deleted = 0
        for i in range(0, len(ids), batch_size):
            batch = ids[i : i + batch_size]
            result = coll.delete_many({"_id": {"$in": batch}})
            deleted += result.deleted_count
        return deleted

    def _delete_missing_in_scope_via_lookup(
        self,
        target,
        staging_coll: str,
        mongo_keys: List[str],
        scope_filter: Dict[str, Any],
    ) -> int:
        """
        Server-side variant: stream the ids of missing documents from a
        $lookup aggregation and delete them in batches, without ever
        materializing the full staged-key set or the full scoped target set
        in this process's memory.
        """
        agg_pipeline = self._build_lookup_missing_pipeline(
            staging_coll, mongo_keys, scope_filter
        )
        deleted = 0
        batch_ids: List[Any] = []
        batch_size = 1000
        for doc in target.aggregate(agg_pipeline, allowDiskUse=True):
            batch_ids.append(doc["_id"])
            if len(batch_ids) >= batch_size:
                deleted += self._delete_by_id_in_batches(target, batch_ids, batch_size)
                batch_ids = []
        if batch_ids:
            deleted += self._delete_by_id_in_batches(target, batch_ids, batch_size)
        return deleted

    def _delete_missing_in_scope_via_python_diff(
        self,
        client,
        schema: str,
        staging_db: str,
        staging_coll: str,
        target,
        mongo_keys: List[str],
        scope_filter: Dict[str, Any],
    ) -> int:
        """
        Fallback for a staging collection in a different database than the
        target ($lookup cannot cross databases on a standard deployment).
        Still loads the full staged-key set into memory (unavoidable without
        a server-side join across databases), but streams the target scope
        via cursor and deletes incrementally instead of materializing every
        id to delete before issuing any delete.
        """
        self.logger.warning(
            "[MongoDBServerWriter.delete_missing_in_scope] staging database '%s' "
            "differs from target database '%s'; $lookup cannot join across "
            "databases here, falling back to a Python-side key diff (loads all "
            "staged keys into memory).",
            staging_db,
            schema,
        )
        staging = client[staging_db][staging_coll]

        staged_keys = set()
        for doc in staging.find({}, {k: 1 for k in mongo_keys}):
            staged_keys.add(tuple(doc.get(k) for k in mongo_keys))

        deleted = 0
        batch_ids: List[Any] = []
        batch_size = 1000
        projection = {k: 1 for k in mongo_keys}
        for doc in target.find(scope_filter, projection):
            key_tuple = tuple(doc.get(k) for k in mongo_keys)
            if key_tuple not in staged_keys:
                batch_ids.append(doc["_id"])
                if len(batch_ids) >= batch_size:
                    deleted += self._delete_by_id_in_batches(target, batch_ids, batch_size)
                    batch_ids = []
        if batch_ids:
            deleted += self._delete_by_id_in_batches(target, batch_ids, batch_size)
        return deleted

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
            target = client[schema][table]
            scope_filter = {mongo_scope: {"$gte": scope_min, "$lte": scope_max}}

            if staging_db == schema:
                # Common case (no staging_schema override): push the whole
                # anti-join down to MongoDB via $lookup, same as the SQL
                # writers' DELETE ... WHERE NOT EXISTS.
                deleted = self._delete_missing_in_scope_via_lookup(
                    target, staging_coll, mongo_keys, scope_filter
                )
            else:
                deleted = self._delete_missing_in_scope_via_python_diff(
                    client, schema, staging_db, staging_coll, target, mongo_keys, scope_filter
                )

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
        self._validate_document_columns(data)

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
