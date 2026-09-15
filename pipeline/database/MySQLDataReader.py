"""
MySQL Data Reader — streams query/table results in batches.
"""
import logging
from typing import Any, Dict, Generator, List, Optional, Tuple

from airflow.exceptions import AirflowException  # type: ignore

from pipeline.core.exceptions import DataReadError, MySQLQueryError
from pipeline.database.MySQLConnectionFactory import MySQLConnectionFactory
from pipeline.database.SQLQueryBuilder import SQLQueryBuilder
from pipeline.utils.IdentifierValidator import IdentifierValidator
from pipeline.interfaces.DataReader import DataReader


class MySQLDataReader(DataReader):
    """
    Reads data from MySQL in streaming mode using Generator.
    Uses chunking to manage memory efficiently.
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
        self.connection_factory = MySQLConnectionFactory(conn_id)
        self.query_builder = SQLQueryBuilder()

    def get_total_count(
        self,
        table_name: Optional[str] = None,
        date_column: Optional[str] = None,
        date_key: Optional[str] = None,
        date_column_type: str = "int",
        custom_query: Optional[str] = None,
        params: Optional[Tuple] = None,
    ) -> int:
        """
        Retrieve total record count via custom COUNT query or table-based builder.
        """
        mode = "custom_query" if custom_query else "query_builder"
        self.logger.info(f"[MySQLDataReader.get_total_count] START | mode={mode} | table={table_name}")
        try:
            if custom_query:
                query = custom_query
                query_params = params
            elif table_name:
                query, query_params = self.query_builder.build_count_query(
                    table_name=table_name,
                    date_column=date_column,
                    date_key=date_key,
                    date_column_type=date_column_type,
                )
            else:
                raise ValueError("Either 'table_name' or 'custom_query' must be provided.")

            result = self.connection_factory.execute_scalar(query, query_params)
            count = int(result) if result is not None else 0
            self.logger.info(f"[MySQLDataReader.get_total_count] SUCCESS | count={count}")
            return count
        except MySQLQueryError:
            raise
        except Exception as exc:
            self.logger.error(f"[MySQLDataReader.get_total_count] Unexpected ERROR | error={exc}", exc_info=True)
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
        Stream table data using keyset pagination (MySQL dialect).
        """
        IdentifierValidator.validate_and_raise(table_name, "table name")
        IdentifierValidator.validate_and_raise(order_by_column, "order by column")
        if date_column:
            IdentifierValidator.validate_and_raise(date_column, "date column")
        if columns:
            IdentifierValidator.validate_columns(columns)

        total_count = self.get_total_count(
            table_name=table_name,
            date_column=date_column,
            date_key=date_key,
            date_column_type=date_column_type,
        )
        if total_count == 0:
            self.logger.warning(f"[MySQLDataReader.stream_data] NO DATA | table={table_name}")
            return

        column_list = "*" if not columns else ", ".join(f"`{c}`" for c in columns)
        last_key = None
        processed = 0
        batch_number = 0

        while processed < total_count:
            batch_number += 1
            conditions: List[str] = []
            parameters: List[Any] = []

            if date_column and date_key:
                conditions.append(f"`{date_column}` = %s")
                if date_column_type == "int":
                    parameters.append(int(date_key))
                else:
                    parameters.append(date_key)

            if last_key is not None:
                conditions.append(f"`{order_by_column}` > %s")
                parameters.append(last_key)

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            quoted_table = ".".join(f"`{part}`" for part in table_name.split("."))
            query = f"""
                SELECT {column_list}
                FROM {quoted_table}
                {where_clause}
                ORDER BY `{order_by_column}`
                LIMIT {int(self.batch_size)}
            """

            try:
                batch = self.connection_factory.execute_query(
                    query,
                    tuple(parameters) if parameters else None,
                )
                if not batch:
                    break

                if order_by_column in batch[-1]:
                    last_key = batch[-1][order_by_column]
                elif order_by_column.lower() in {k.lower(): k for k in batch[-1]}:
                    key_map = {k.lower(): k for k in batch[-1]}
                    last_key = batch[-1][key_map[order_by_column.lower()]]

                processed += len(batch)
                self.logger.info(
                    f"[MySQLDataReader.stream_data] Batch {batch_number} | rows={len(batch)} | processed={processed}/{total_count}"
                )
                yield batch

                if processed >= total_count or len(batch) < self.batch_size:
                    break
            except Exception as exc:
                self.logger.error(
                    f"[MySQLDataReader.stream_data] ERROR reading batch {batch_number} | error={exc}",
                    exc_info=True
                )
                raise AirflowException(f"MySQL read error: {exc}") from exc

    def stream_query(
        self,
        query: str,
        count_query: str = None,
        parameters: Optional[tuple] = None,
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Stream results of an arbitrary SQL query using fetchmany.
        """
        processed = 0
        batch_number = 0

        self.logger.info(
            f"[MySQLDataReader.stream_query] START | batch_size={self.batch_size} | has_count_query={count_query is not None}"
        )

        if count_query is not None:
            total_count = self.get_total_count(custom_query=count_query, params=parameters)
            self.logger.info(f"[MySQLDataReader.stream_query] Total rows to stream: {total_count:,}")
        else:
            total_count = None

        try:
            with self.connection_factory.get_cursor(as_dict=True) as cursor:
                if parameters:
                    cursor.execute(query, parameters)
                else:
                    cursor.execute(query)

                while True:
                    batch = cursor.fetchmany(self.batch_size)
                    if not batch:
                        break

                    if batch and not isinstance(batch[0], dict):
                        description = getattr(cursor, "description", None)
                        columns = [col[0] for col in description] if description else []
                        batch = [dict(zip(columns, row)) for row in batch]

                    processed += len(batch)
                    batch_number += 1

                    if total_count:
                        percentage = processed / total_count * 100
                        self.logger.info(
                            f"[MySQLDataReader.stream_query] Batch {batch_number} | rows={len(batch)} | processed={processed:,}/{total_count:,} ({percentage:.1f}%)"
                        )
                    else:
                        self.logger.info(
                            f"[MySQLDataReader.stream_query] Batch {batch_number} | rows={len(batch)} | processed={processed:,}"
                        )

                    yield batch
        except Exception as exc:
            self.logger.error(
                f"[MySQLDataReader.stream_query] ERROR | processed={processed} | error={exc}",
                exc_info=True
            )
            raise AirflowException(f"Streaming MySQL query failed: {exc}") from exc

        self.logger.info(f"[MySQLDataReader.stream_query] FINISH | processed={processed} | batches={batch_number}")
