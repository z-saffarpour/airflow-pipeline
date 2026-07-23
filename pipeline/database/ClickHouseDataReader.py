"""
ClickHouse Data Reader — streams query/table results in batches.
"""
import logging
from typing import Any, Dict, Generator, List, Optional, Union

from airflow.exceptions import AirflowException  # type: ignore

from pipeline.core.exceptions import DataReadError
from pipeline.database.ClickHouseConnectionFactory import ClickHouseConnectionFactory
from pipeline.database.SQLQueryBuilder import SQLQueryBuilder
from pipeline.interfaces.DataReader import DataReader

ParamsType = Optional[Union[dict, tuple]]


class ClickHouseDataReader(DataReader):
    """
    Reads data from ClickHouse in streaming mode using Generator.
    Uses execute_iter for memory-efficient batching.
    Implements DataReader for dependency inversion.
    """

    def __init__(
        self,
        conn_id: str,
        batch_size: int = 50000,
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.conn_id = conn_id
        self.batch_size = batch_size
        self.connection_factory = ClickHouseConnectionFactory(conn_id)
        self.query_builder = SQLQueryBuilder()

    @staticmethod
    def _normalize_params(parameters: ParamsType) -> dict:
        """Convert tuple params to a dict for clickhouse_driver, or pass dict through."""
        if parameters is None:
            return {}
        if isinstance(parameters, dict):
            return parameters
        raise TypeError(
            "ClickHouse parameters must be a dict (named params, e.g. "
            "{'min_key': ..., 'max_key': ...}); got "
            f"{type(parameters).__name__}"
        )

    def get_total_count(
        self,
        table_name: Optional[str] = None,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = "int",
        custom_query: Optional[str] = None,
        params: ParamsType = None,
    ) -> int:
        """
        Retrieve total record count via custom COUNT query or table-based filter.
        """
        mode = "custom_query" if custom_query else "table"
        self.logger.info(
            "[ClickHouseDataReader.get_total_count] START | mode=%s | table=%s",
            mode,
            table_name,
        )
        try:
            query_params = self._normalize_params(params)
            if custom_query:
                query = custom_query
            elif table_name:
                SQLQueryBuilder._validate_and_raise(table_name, "table name")
                quoted_table = ".".join(
                    f"`{part}`" for part in table_name.split(".")
                )
                conditions: List[str] = []
                if date_column and date_key is not None:
                    SQLQueryBuilder._validate_and_raise(date_column, "date column")
                    conditions.append(f"`{date_column}` = %(date_key)s")
                    if date_column_type == "int":
                        query_params["date_key"] = int(date_key)
                    else:
                        query_params["date_key"] = date_key
                where_clause = (
                    f"WHERE {' AND '.join(conditions)}" if conditions else ""
                )
                query = f"SELECT count() FROM {quoted_table} {where_clause}"
            else:
                raise ValueError(
                    "Either 'table_name' or 'custom_query' must be provided."
                )

            result = self.connection_factory.execute_scalar(query, query_params)
            count = int(result) if result is not None else 0
            self.logger.info(
                "[ClickHouseDataReader.get_total_count] SUCCESS | count=%s",
                count,
            )
            return count
        except Exception as exc:
            self.logger.error(
                "[ClickHouseDataReader.get_total_count] Unexpected ERROR | error=%s",
                exc,
                exc_info=True,
            )
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
        Stream table data using keyset pagination (ClickHouse dialect).
        """
        SQLQueryBuilder._validate_and_raise(table_name, "table name")
        SQLQueryBuilder._validate_and_raise(order_by_column, "order by column")
        if date_column:
            SQLQueryBuilder._validate_and_raise(date_column, "date column")
        if columns:
            SQLQueryBuilder._validate_columns(columns)

        total_count = self.get_total_count(
            table_name=table_name,
            date_column=date_column,
            date_key=date_key,
            date_column_type=date_column_type,
        )
        if total_count == 0:
            self.logger.warning(
                "[ClickHouseDataReader.stream_data] NO DATA | table=%s",
                table_name,
            )
            return

        column_list = (
            "*" if not columns else ", ".join(f"`{c}`" for c in columns)
        )
        quoted_table = ".".join(f"`{part}`" for part in table_name.split("."))
        last_key = None
        processed = 0
        batch_number = 0

        while processed < total_count:
            batch_number += 1
            conditions: List[str] = []
            parameters: Dict[str, Any] = {}

            if date_column and date_key is not None:
                conditions.append(f"`{date_column}` = %(date_key)s")
                if date_column_type == "int":
                    parameters["date_key"] = int(date_key)
                else:
                    parameters["date_key"] = date_key

            if last_key is not None:
                conditions.append(f"`{order_by_column}` > %(last_key)s")
                parameters["last_key"] = last_key

            where_clause = (
                f"WHERE {' AND '.join(conditions)}" if conditions else ""
            )
            query = f"""
                SELECT {column_list}
                FROM {quoted_table}
                {where_clause}
                ORDER BY `{order_by_column}`
                LIMIT {int(self.batch_size)}
            """

            try:
                batch = self.connection_factory.execute_query_as_dicts(
                    query,
                    parameters,
                )
                if not batch:
                    break

                if order_by_column in batch[-1]:
                    last_key = batch[-1][order_by_column]
                else:
                    key_map = {k.lower(): k for k in batch[-1]}
                    if order_by_column.lower() in key_map:
                        last_key = batch[-1][key_map[order_by_column.lower()]]

                processed += len(batch)
                self.logger.info(
                    "[ClickHouseDataReader.stream_data] Batch %s | rows=%s | "
                    "processed=%s/%s",
                    batch_number,
                    len(batch),
                    processed,
                    total_count,
                )
                yield batch

                if processed >= total_count or len(batch) < self.batch_size:
                    break
            except Exception as exc:
                self.logger.error(
                    "[ClickHouseDataReader.stream_data] ERROR reading batch %s | "
                    "error=%s",
                    batch_number,
                    exc,
                    exc_info=True,
                )
                raise AirflowException(f"ClickHouse read error: {exc}") from exc

    def stream_query(
        self,
        query: str,
        count_query: str = None,
        parameters: ParamsType = None,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Stream results of an arbitrary SQL query using execute_iter.
        """
        processed = 0
        batch_number = 0
        query_params = self._normalize_params(parameters)

        self.logger.info(
            "[ClickHouseDataReader.stream_query] START | batch_size=%s | "
            "has_count_query=%s",
            self.batch_size,
            count_query is not None,
        )

        if count_query is not None:
            total_count = self.get_total_count(
                custom_query=count_query,
                params=query_params,
            )
            self.logger.info(
                "[ClickHouseDataReader.stream_query] Total rows to stream: %s",
                f"{total_count:,}",
            )
        else:
            total_count = None

        try:
            with self.connection_factory.get_connection() as client:
                row_iter = client.execute_iter(
                    query,
                    query_params,
                    with_column_types=True,
                )
                columns_with_types = next(row_iter)
                column_names = [col[0] for col in columns_with_types]

                batch: List[Dict[str, Any]] = []
                for row in row_iter:
                    batch.append(dict(zip(column_names, row)))
                    if len(batch) >= self.batch_size:
                        processed += len(batch)
                        batch_number += 1
                        self._log_stream_batch(
                            batch_number,
                            len(batch),
                            processed,
                            total_count,
                        )
                        yield batch
                        batch = []

                if batch:
                    processed += len(batch)
                    batch_number += 1
                    self._log_stream_batch(
                        batch_number,
                        len(batch),
                        processed,
                        total_count,
                    )
                    yield batch
        except Exception as exc:
            self.logger.error(
                "[ClickHouseDataReader.stream_query] ERROR | processed=%s | "
                "error=%s",
                processed,
                exc,
                exc_info=True,
            )
            raise AirflowException(
                f"Streaming ClickHouse query failed: {exc}"
            ) from exc

        self.logger.info(
            "[ClickHouseDataReader.stream_query] FINISH | processed=%s | "
            "batches=%s",
            processed,
            batch_number,
        )

    def _log_stream_batch(
        self,
        batch_number: int,
        batch_len: int,
        processed: int,
        total_count: Optional[int],
    ) -> None:
        if total_count:
            percentage = processed / total_count * 100
            self.logger.info(
                "[ClickHouseDataReader.stream_query] Batch %s | rows=%s | "
                "processed=%s/%s (%.1f%%)",
                batch_number,
                batch_len,
                f"{processed:,}",
                f"{total_count:,}",
                percentage,
            )
        else:
            self.logger.info(
                "[ClickHouseDataReader.stream_query] Batch %s | rows=%s | "
                "processed=%s",
                batch_number,
                batch_len,
                f"{processed:,}",
            )
