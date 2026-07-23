"""
PostgreSQL Writer with batch upsert via staging tables.
Designed for Airflow ETL pipelines (MSSQL → PostgreSQL sync and similar).
"""
import logging
from typing import Any, Dict, List, Optional, Tuple

from pipeline.database.PostgreSQLConnectionFactory import PostgreSQLConnectionFactory
from pipeline.database.SQLQueryBuilder import SQLQueryBuilder
from pipeline.interfaces.DataWriter import DataWriter
from pipeline.utils.retry_helper import RetryContext

_UPSERT_DEADLOCK_MAX_ATTEMPTS = 5
_UPSERT_DEADLOCK_BASE_DELAY = 2.0
_UPSERT_DEADLOCK_MAX_DELAY = 30.0


def _is_postgres_deadlock(exc: BaseException) -> bool:
    """Return True if exc looks like a PostgreSQL deadlock (SQLSTATE 40P01)."""
    pgcode = getattr(exc, "pgcode", None)
    if pgcode == "40P01":
        return True
    message = str(exc).lower()
    return "deadlock" in message and ("40p01" in message or "detected" in message)


class PostgreSQLServerWriter(DataWriter):
    """
    PostgreSQL writer with batch upsert via staging tables.

    Features:
    - Staging-based INSERT / UPDATE / DELETE / UPSERT
    - Optional unique-key conflict cleanup
    - Hash-based change detection (md5)
    - Scoped delete_missing via persistent keys staging
    """

    def __init__(self, conn_id: str, autocommit: bool = False) -> None:
        self.conn_id = conn_id
        self.autocommit = autocommit
        self.connection_factory = PostgreSQLConnectionFactory(conn_id)
        self.logger = logging.getLogger(self.__class__.__name__)
        self._cached_unique_keys: Dict[str, Tuple[Tuple[str, ...], ...]] = {}

    @staticmethod
    def _quote_ident(identifier: str) -> str:
        SQLQueryBuilder._validate_and_raise(identifier, "identifier")
        return f'"{identifier}"'

    @classmethod
    def _quote_table(cls, schema: str, table: str) -> str:
        return f"{cls._quote_ident(schema)}.{cls._quote_ident(table)}"

    @staticmethod
    def _build_null_safe_equality(
        columns: List[str],
        left_alias: str,
        right_alias: str,
    ) -> str:
        conditions = []
        for column in columns:
            left = f'{left_alias}."{column}"'
            right = f'{right_alias}."{column}"'
            conditions.append(
                f"({left} = {right} OR ({left} IS NULL AND {right} IS NULL))"
            )
        return " AND ".join(conditions)

    @staticmethod
    def _build_row_hash_expression(columns: List[str], table_alias: str) -> str:
        parts = [
            f"COALESCE(CAST({table_alias}.\"{column}\" AS TEXT), 'NULL')"
            for column in columns
        ]
        return f"md5(concat_ws('|', {', '.join(parts)}))"

    def _staging_table_name(self, staging_schema: str, table: str, suffix: str) -> str:
        return self._quote_table(staging_schema, f"{table}_staging_{suffix}")

    def _keys_staging_table_name(
        self, staging_schema: str, table: str, suffix: str
    ) -> str:
        return self._quote_table(staging_schema, f"{table}_sync_keys_{suffix}")

    def _discover_unique_keys(
        self,
        schema: str,
        table: str,
    ) -> Tuple[Tuple[str, ...], ...]:
        cache_key = f"{schema}.{table}".lower()
        if cache_key in self._cached_unique_keys:
            return self._cached_unique_keys[cache_key]

        SQLQueryBuilder._validate_and_raise(f"{schema}.{table}", "table name")

        query = """
        SELECT
            tc.constraint_name AS index_name,
            kcu.column_name AS column_name,
            kcu.ordinal_position AS key_ordinal
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_schema = kcu.constraint_schema
         AND tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'UNIQUE'
          AND tc.table_schema = %s
          AND tc.table_name = %s
        ORDER BY tc.constraint_name, kcu.ordinal_position
        """
        rows = self.connection_factory.execute_query(
            query,
            parameters=(schema, table),
        )

        indexes: Dict[str, List[str]] = {}
        for row in rows:
            index_name = row.get("index_name") or row.get("INDEX_NAME")
            column_name = row.get("column_name") or row.get("COLUMN_NAME")
            if not index_name or not column_name:
                continue
            indexes.setdefault(str(index_name), []).append(str(column_name))

        unique_keys = tuple(tuple(columns) for columns in indexes.values())
        self._cached_unique_keys[cache_key] = unique_keys

        if unique_keys:
            self.logger.info(
                "[PostgreSQLServerWriter._discover_unique_keys] Discovered %d unique key(s) "
                "for %s.%s: %s",
                len(unique_keys),
                schema,
                table,
                unique_keys,
            )
        return unique_keys

    def _resolve_unique_keys(
        self,
        schema: str,
        table: str,
        unique_keys: Optional[Tuple[Tuple[str, ...], ...]],
        resolve_unique_key_conflicts: bool,
    ) -> Tuple[Tuple[str, ...], ...]:
        if unique_keys:
            return unique_keys
        if resolve_unique_key_conflicts:
            return self._discover_unique_keys(schema, table)
        return ()

    def _delete_unique_key_conflicts(
        self,
        cursor,
        table_full: str,
        staging_table: str,
        key_columns: List[str],
        unique_keys: Tuple[Tuple[str, ...], ...],
        available_columns: List[str],
    ) -> int:
        if not unique_keys:
            return 0

        available = {column.upper() for column in available_columns}
        primary_key_match = self._build_null_safe_equality(
            key_columns, "target", "source"
        )
        primary_key_mismatch = f"NOT ({primary_key_match})"
        total_deleted = 0

        for unique_key in unique_keys:
            filtered_columns = [
                column for column in unique_key if column.upper() in available
            ]
            if len(filtered_columns) != len(unique_key):
                continue

            for column in filtered_columns:
                SQLQueryBuilder._validate_and_raise(column, "column name")

            unique_key_match = self._build_null_safe_equality(
                filtered_columns, "target", "source"
            )
            delete_query = f"""
            DELETE FROM {table_full} AS target
            USING {staging_table} AS source
            WHERE {unique_key_match}
              AND {primary_key_mismatch}
            """
            cursor.execute(delete_query)
            deleted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            if deleted:
                total_deleted += deleted
                self.logger.info(
                    "[PostgreSQLServerWriter._delete_unique_key_conflicts] Deleted %d "
                    "conflicting row(s) from %s for unique key (%s)",
                    deleted,
                    table_full,
                    ", ".join(filtered_columns),
                )
        return total_deleted

    def _drop_staging_table(self, cursor, staging_table: str) -> None:
        cursor.execute(f"DROP TABLE IF EXISTS {staging_table}")

    def _prepare_staging_table(self, cursor, staging_table: str, table_full: str) -> None:
        self._drop_staging_table(cursor, staging_table)
        cursor.execute(
            f"CREATE TABLE {staging_table} (LIKE {table_full} INCLUDING DEFAULTS)"
        )

    def _load_staging_rows(
        self,
        cursor,
        staging_table: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        batch_size: int,
    ) -> None:
        placeholders = ", ".join(["%s" for _ in columns])
        column_list = ", ".join(self._quote_ident(col) for col in columns)
        insert_query = (
            f"INSERT INTO {staging_table} ({column_list}) VALUES ({placeholders})"
        )
        for i in range(0, len(data), batch_size):
            batch = data[i : i + batch_size]
            values = [tuple(row[col] for col in columns) for row in batch]
            cursor.executemany(insert_query, values)

    def _cleanup_staging_table(self, conn, cursor, staging_table: str) -> None:
        try:
            self._drop_staging_table(cursor, staging_table)
            if not self.autocommit:
                conn.commit()
        except Exception as exc:
            if not self.autocommit:
                conn.rollback()
            self.logger.warning(
                "[PostgreSQLServerWriter._cleanup_staging_table] Failed to drop %s: %s",
                staging_table,
                exc,
            )

    def _finalize_connection(self, conn, cursor, *, commit: bool = True) -> None:
        try:
            if commit:
                if not self.autocommit:
                    conn.commit()
            elif not self.autocommit:
                conn.rollback()
        except Exception as exc:
            self.logger.warning(
                "[PostgreSQLServerWriter._finalize_connection] Commit/rollback failed: %s",
                exc,
            )
        try:
            cursor.close()
        except Exception:
            pass

    def _load_staging_table(
        self,
        staging_table: str,
        table_full: str,
        data: List[Dict[str, Any]],
        columns: List[str],
        batch_size: int,
    ) -> None:
        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            success = False
            try:
                self._prepare_staging_table(cursor, staging_table, table_full)
                self._load_staging_rows(cursor, staging_table, data, columns, batch_size)
                success = True
            finally:
                self._finalize_connection(conn, cursor, commit=success)

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

        SQLQueryBuilder._validate_and_raise(f"{schema}.{table}", "table name")
        effective_staging_schema = staging_schema or schema
        table_full = self._quote_table(schema, table)
        staging_table = self._staging_table_name(
            effective_staging_schema, table, "insert"
        )
        columns = list(data[0].keys())
        column_list = ", ".join(self._quote_ident(col) for col in columns)
        insert_query = (
            f"INSERT INTO {table_full} ({column_list}) "
            f"SELECT {column_list} FROM {staging_table}"
        )

        self._load_staging_table(staging_table, table_full, data, columns, batch_size)

        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            success = False
            try:
                cursor.execute(insert_query)
                total_inserted = (
                    cursor.rowcount
                    if cursor.rowcount and cursor.rowcount > 0
                    else len(data)
                )
                success = True
                return total_inserted
            finally:
                self._cleanup_staging_table(conn, cursor, staging_table)
                self._finalize_connection(conn, cursor, commit=success)

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

        SQLQueryBuilder._validate_and_raise(f"{schema}.{table}", "table name")
        for kc in key_columns:
            SQLQueryBuilder._validate_and_raise(kc, "column name")

        effective_staging_schema = staging_schema or schema
        table_full = self._quote_table(schema, table)
        staging_table = self._staging_table_name(
            effective_staging_schema, table, "update"
        )
        columns = [col for col in data[0].keys() if col not in key_columns]
        set_clause = ", ".join(
            f'target."{col}" = source."{col}"' for col in columns
        )
        join_clause = " AND ".join(
            f'target."{col}" = source."{col}"' for col in key_columns
        )
        update_query = f"""
        UPDATE {table_full} AS target
        SET {set_clause}
        FROM {staging_table} AS source
        WHERE {join_clause}
        """

        self._load_staging_table(
            staging_table, table_full, data, list(data[0].keys()), batch_size
        )

        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            success = False
            try:
                cursor.execute(update_query)
                total_updated = (
                    cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                )
                success = True
                return total_updated
            finally:
                self._cleanup_staging_table(conn, cursor, staging_table)
                self._finalize_connection(conn, cursor, commit=success)

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

        SQLQueryBuilder._validate_and_raise(f"{schema}.{table}", "table name")
        SQLQueryBuilder._validate_and_raise(key_column, "column name")

        effective_staging_schema = staging_schema or schema
        table_full = self._quote_table(schema, table)
        staging_table = self._staging_table_name(
            effective_staging_schema, table, "delete"
        )
        quoted_key = self._quote_ident(key_column)
        delete_query = f"""
        DELETE FROM {table_full} AS target
        USING {staging_table} AS source
        WHERE target.{quoted_key} = source.{quoted_key}
        """
        staging_insert = f"INSERT INTO {staging_table} ({quoted_key}) VALUES (%s)"

        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            success = False
            try:
                self._drop_staging_table(cursor, staging_table)
                cursor.execute(
                    f"CREATE TABLE {staging_table} AS "
                    f"SELECT {quoted_key} FROM {table_full} WHERE FALSE"
                )
                for i in range(0, len(key_values), batch_size):
                    batch = key_values[i : i + batch_size]
                    cursor.executemany(staging_insert, [(value,) for value in batch])
                success = True
            finally:
                self._finalize_connection(conn, cursor, commit=success)

        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            success = False
            try:
                cursor.execute(delete_query)
                total_deleted = (
                    cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                )
                success = True
                return total_deleted
            finally:
                self._cleanup_staging_table(conn, cursor, staging_table)
                self._finalize_connection(conn, cursor, commit=success)

    def prepare_keys_staging_table(
        self,
        schema: str,
        table: str,
        key_columns: List[str],
        suffix: str,
        staging_schema: Optional[str] = None,
    ) -> str:
        SQLQueryBuilder._validate_and_raise(f"{schema}.{table}", "table name")
        for column in key_columns:
            SQLQueryBuilder._validate_and_raise(column, "column name")
        SQLQueryBuilder._validate_and_raise(suffix, "staging suffix")

        effective_staging_schema = staging_schema or schema
        table_full = self._quote_table(schema, table)
        keys_staging_table = self._keys_staging_table_name(
            effective_staging_schema, table, suffix
        )
        key_column_list = ", ".join(self._quote_ident(col) for col in key_columns)

        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            try:
                self._drop_staging_table(cursor, keys_staging_table)
                cursor.execute(
                    f"CREATE TABLE {keys_staging_table} AS "
                    f"SELECT {key_column_list} FROM {table_full} WHERE FALSE"
                )
                if not self.autocommit:
                    conn.commit()
                self.logger.info(
                    "[PostgreSQLServerWriter.prepare_keys_staging_table] Prepared %s for %s",
                    keys_staging_table,
                    table_full,
                )
                return keys_staging_table
            except Exception:
                if not self.autocommit:
                    conn.rollback()
                raise
            finally:
                cursor.close()

    def append_keys_to_staging(
        self,
        keys_staging_table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int = 1000,
    ) -> int:
        if not data:
            return 0

        for column in key_columns:
            SQLQueryBuilder._validate_and_raise(column, "column name")

        column_list = ", ".join(self._quote_ident(col) for col in key_columns)
        placeholders = ", ".join(["%s" for _ in key_columns])
        insert_query = (
            f"INSERT INTO {keys_staging_table} ({column_list}) "
            f"VALUES ({placeholders})"
        )

        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            success = False
            try:
                total_appended = 0
                for i in range(0, len(data), batch_size):
                    batch = data[i : i + batch_size]
                    values = [
                        tuple(row[col] for col in key_columns) for row in batch
                    ]
                    cursor.executemany(insert_query, values)
                    total_appended += len(batch)
                success = True
                return total_appended
            finally:
                self._finalize_connection(conn, cursor, commit=success)

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
        SQLQueryBuilder._validate_and_raise(f"{schema}.{table}", "table name")
        for column in key_columns:
            SQLQueryBuilder._validate_and_raise(column, "column name")
        SQLQueryBuilder._validate_and_raise(scope_column, "column name")

        if min_key is None or max_key is None:
            self.logger.warning(
                "[PostgreSQLServerWriter.delete_missing_in_scope] Skipping scoped delete "
                "for %s.%s; min_key or max_key is NULL",
                schema,
                table,
            )
            return 0

        table_full = self._quote_table(schema, table)
        key_match = self._build_null_safe_equality(key_columns, "target", "source")
        quoted_scope = self._quote_ident(scope_column)
        delete_query = f"""
        DELETE FROM {table_full} AS target
        WHERE target.{quoted_scope} >= %s
          AND target.{quoted_scope} <= %s
          AND NOT EXISTS (
              SELECT 1
              FROM {keys_staging_table} AS source
              WHERE {key_match}
          )
        """

        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(delete_query, (min_key, max_key))
                deleted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                if not self.autocommit:
                    conn.commit()
                self.logger.info(
                    "[PostgreSQLServerWriter.delete_missing_in_scope] Scoped delete for %s | "
                    "scope=%s | min=%s | max=%s | deleted=%d",
                    table_full,
                    scope_column,
                    min_key,
                    max_key,
                    deleted,
                )
                return deleted
            except Exception:
                if not self.autocommit:
                    conn.rollback()
                raise
            finally:
                cursor.close()

    def drop_keys_staging_table(self, keys_staging_table: str) -> None:
        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            try:
                self._drop_staging_table(cursor, keys_staging_table)
                if not self.autocommit:
                    conn.commit()
            except Exception:
                if not self.autocommit:
                    conn.rollback()
                raise
            finally:
                cursor.close()

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
            self.logger.warning("[PostgreSQLServerWriter.upsert_batch] No data to upsert")
            return {"inserted": 0, "updated": 0, "deleted": 0}

        SQLQueryBuilder._validate_and_raise(f"{schema}.{table}", "table name")
        for kc in key_columns:
            SQLQueryBuilder._validate_and_raise(kc, "column name")
        SQLQueryBuilder._validate_and_raise(staging_suffix, "staging suffix")

        effective_staging_schema = staging_schema or schema
        table_full = self._quote_table(schema, table)
        staging_table = self._staging_table_name(
            effective_staging_schema, table, staging_suffix
        )

        retry_ctx = RetryContext(
            max_attempts=_UPSERT_DEADLOCK_MAX_ATTEMPTS,
            base_delay=_UPSERT_DEADLOCK_BASE_DELAY,
            max_delay=_UPSERT_DEADLOCK_MAX_DELAY,
        )
        while True:
            try:
                return self._upsert_batch_once(
                    schema=schema,
                    table=table,
                    data=data,
                    key_columns=key_columns,
                    batch_size=batch_size,
                    unique_keys=unique_keys,
                    resolve_unique_key_conflicts=resolve_unique_key_conflicts,
                    use_hash_change_detection=use_hash_change_detection,
                    table_full=table_full,
                    staging_table=staging_table,
                )
            except Exception as exc:
                if not _is_postgres_deadlock(exc):
                    raise
                attempt = retry_ctx.current_attempt + 1
                if attempt >= retry_ctx.max_attempts:
                    self.logger.error(
                        "[PostgreSQLServerWriter.upsert_batch] Deadlock persisted after "
                        "%s attempts for %s: %s",
                        retry_ctx.max_attempts,
                        table_full,
                        exc,
                    )
                    raise
                self.logger.warning(
                    "[PostgreSQLServerWriter.upsert_batch] Deadlock victim on attempt "
                    "%s/%s for %s, retrying...",
                    attempt,
                    retry_ctx.max_attempts,
                    table_full,
                )
                retry_ctx.record_failure(exc)

    def _upsert_batch_once(
        self,
        schema: str,
        table: str,
        data: List[Dict[str, Any]],
        key_columns: List[str],
        batch_size: int,
        unique_keys: Optional[Tuple[Tuple[str, ...], ...]],
        resolve_unique_key_conflicts: bool,
        use_hash_change_detection: bool,
        table_full: str,
        staging_table: str,
    ) -> Dict[str, int]:
        columns = list(data[0].keys())
        resolved_unique_keys = self._resolve_unique_keys(
            schema,
            table,
            unique_keys,
            resolve_unique_key_conflicts,
        )
        update_columns = [col for col in columns if col not in key_columns]
        match_conditions = self._build_null_safe_equality(
            key_columns, "target", "source"
        )

        self._load_staging_table(
            staging_table, table_full, data, columns, batch_size
        )

        with self.connection_factory.get_connection() as conn:
            cursor = conn.cursor()
            success = False
            try:
                self._delete_unique_key_conflicts(
                    cursor,
                    table_full,
                    staging_table,
                    key_columns,
                    resolved_unique_keys,
                    columns,
                )

                updated_count = 0
                if update_columns:
                    set_clause = ", ".join(
                        f'target."{col}" = source."{col}"' for col in update_columns
                    )
                    if use_hash_change_detection:
                        change_condition = (
                            f"{self._build_row_hash_expression(update_columns, 'source')} "
                            f"<> {self._build_row_hash_expression(update_columns, 'target')}"
                        )
                    else:
                        change_condition = " OR ".join(
                            f"""(
                                target."{col}" <> source."{col}"
                                OR (target."{col}" IS NULL AND source."{col}" IS NOT NULL)
                                OR (target."{col}" IS NOT NULL AND source."{col}" IS NULL)
                            )"""
                            for col in update_columns
                        )

                    update_query = f"""
                    UPDATE {table_full} AS target
                    SET {set_clause}
                    FROM {staging_table} AS source
                    WHERE {match_conditions}
                      AND ({change_condition})
                    """
                    cursor.execute(update_query)
                    updated_count = (
                        cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                    )

                insert_columns = ", ".join(self._quote_ident(col) for col in columns)
                insert_select = ", ".join(f'source."{col}"' for col in columns)
                insert_query = f"""
                INSERT INTO {table_full} ({insert_columns})
                SELECT {insert_select}
                FROM {staging_table} AS source
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {table_full} AS target
                    WHERE {match_conditions}
                )
                """
                cursor.execute(insert_query)
                inserted_count = (
                    cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
                )
                success = True

                self.logger.info(
                    "[PostgreSQLServerWriter.upsert_batch] %s sync completed | staging=%s | "
                    "inserted=%s | updated=%s | hash_change_detection=%s",
                    table_full,
                    staging_table,
                    inserted_count,
                    updated_count,
                    use_hash_change_detection,
                )
                return {
                    "success": True,
                    "inserted": inserted_count,
                    "updated": updated_count,
                    "deleted": 0,
                    "total": inserted_count + updated_count,
                }
            except Exception as exc:
                self.logger.error(
                    "[PostgreSQLServerWriter.upsert_batch] Upsert failed for %s: %s",
                    table_full,
                    exc,
                )
                raise
            finally:
                self._cleanup_staging_table(conn, cursor, staging_table)
                self._finalize_connection(conn, cursor, commit=success)
