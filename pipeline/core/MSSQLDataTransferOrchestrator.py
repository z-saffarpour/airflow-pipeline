"""
Data Transfer Orchestrator - High-level orchestration of data transfer operations.
Coordinates between different components following SOLID principles.
"""
import logging
from datetime import datetime
from typing import Optional

from pipeline.core.TransferMetrics import TransferMetrics
from pipeline.core.TransferResult import TransferResult
from pipeline.core.exceptions import KafkaProducerError, DataReadError

from pipeline.config.TableConfiguration import TableConfiguration
from pipeline.config.QueryConfiguration import QueryConfiguration

from pipeline.database.MSSQLDataReader import MSSQLDataReader
from pipeline.database.ClickHouseWriter import ClickHouseWriter
from pipeline.kafka.IdempotentKafkaProducer import IdempotentKafkaProducer

class MSSQLDataTransferOrchestrator:
    """
    Orchestrates data transfer from SQL Server to Kafka.
    
    Coordinates between MSSQLDataReader, KafkaProducer, and other components
    following SOLID principles and dependency inversion.
    """
    
    def __init__(
        self, 
        mssql_conn_id: str, 
        kafka_bootstrap_servers: str, 
        clickhouse_conn_id: str, 
        is_send_kafka: bool ,
        is_send_clickhouse: bool,
        fail_on_error: bool,
        is_connection_string: bool = False,
        version: str = None
        ) -> None:
        """
        Initialize orchestrator with connection parameters.

        Args:
            mssql_conn_id: Airflow connection ID for SQL Server
            kafka_bootstrap_servers: Kafka bootstrap servers
            is_connection_string: If True, mssql_conn_id is treated as a connection string
        """
        self.mssql_conn_id = mssql_conn_id
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.clickhouse_conn_id = clickhouse_conn_id
        self.is_send_kafka= is_send_kafka
        self.is_send_clickhouse= is_send_clickhouse
        self.fail_on_error= fail_on_error
        self.is_connection_string = is_connection_string
        if version is None:
            self.version = datetime.now().strftime('%Y%m%d%H%M%S')
        else:
            self.version = version
            
        self.logger = logging.getLogger(self.__class__.__name__)

    def transfer_table_data(
        self,
        config: TableConfiguration,
        execution_date: str,
        batch_size: int,
        kafka_topic: Optional[str]=None,
        clickhouse_database : Optional[str]=None,
        clickhouse_table_name : Optional[str]=None,
        
    ) -> TransferResult:
        """
        Transfer data from SQL Server table to Kafka topic.

        Args:
            config: Table configuration
            execution_date: Execution date in YYYYMMDD format

        Returns:
            TransferResult with transfer statistics

        Raises:
            AirflowException: If transfer fails
        """
        # Initialize metrics
        metrics = TransferMetrics(
            table_name=config.table_name,
            execution_date=execution_date,
        )

        self.logger.info(
            "Starting data transfer",
            extra={
                "table_name": config.table_name,
                "kafka_topic": kafka_topic,
                "execution_date": execution_date,
                "batch_size": batch_size,
                "has_columns_filter": config.columns is not None
            }
        )

        # Initialize components
        producer = None
        reader = None
        clickhouse_writer = None

        try:
            # Initialize producer with client_id based on table name
            if self.is_send_kafka:
                client_id = f"airflow-{config.table_name.replace('.', '-')}"
                producer = IdempotentKafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    client_id=client_id,
                )
                
            if self.is_send_clickhouse:
                clickhouse_writer = ClickHouseWriter(self.clickhouse_conn_id)
            
            # Initialize reader
            reader = MSSQLDataReader(
                conn_id=self.mssql_conn_id,
                batch_size=batch_size,
                is_connection_string=self.is_connection_string,
            )

            # Get total count for metrics
            total_count = reader.get_total_count(
                table_name=config.table_name,
                date_column=config.date_column,
                date_key=execution_date if config.date_column else None,
                date_column_type=config.date_column_type,
            )
            metrics.total_records = total_count

            self.logger.info(
                "Total records to transfer",
                extra={
                    "table_name": config.table_name,
                    "total_count": total_count,
                    "execution_date": execution_date
                }
            )

            if total_count == 0:
                self.logger.info(
                    "No records to transfer",
                    extra={"table_name": config.table_name, "execution_date": execution_date}
                )
                metrics.mark_completed()
                return TransferResult.create_success(
                    records_transferred=0,
                    batch_count=0,
                    duration_seconds=metrics.duration_seconds,
                    total_records=total_count,
                )

            # Stream and transfer data
            batch_number = 0
            for batch in reader.stream_data(
                table_name=config.table_name,
                order_by_column=config.order_by_column,
                columns=config.columns,
                date_column=config.date_column,
                date_key=execution_date if config.date_column else None,
                date_column_type=config.date_column_type,
            ):
                batch_number += 1
                batch_size = len(batch)

                # Send batch
                if self.is_send_kafka and producer:
                    try:
                        self.logger.info(f"kafka broker : {self.kafka_bootstrap_servers}")
                        producer.send_batch_to_kafka(
                            batch=batch,
                            topic=kafka_topic,
                            source_name=config.table_name,
                            key_column=config.order_by_column,
                            execution_date=execution_date,
                            batch_number=batch_number,
                            version=self.version
                        )
                        self.logger.debug(f"Batch {batch_number} flushed to Kafka ({len(batch)} records)")
                    except Exception as e:
                        self.logger.error(f"Kafka send failed: {e}")
                        if self.fail_on_error:
                            raise
                        
                if self.is_send_clickhouse and clickhouse_writer:
                    try:
                        clickhouse_writer.upsert_batch(clickhouse_database, clickhouse_table_name, batch, self.version)
                        self.logger.debug(f"Batch {batch_number} Inserted to clickhouse ({len(batch)} records)")
                    except Exception as e:
                        self.logger.error(f"ClickHouse write failed: {e}")
                        if self.fail_on_error:
                            raise

                # Update metrics
                metrics.increment_batch(batch_size)

                # Log progress
                progress = metrics.get_progress_percentage()
                self.logger.info(
                    "Batch transferred",
                    extra={
                        "batch_number": batch_number,
                        "batch_size": batch_size,
                        "transferred": metrics.transferred_records,
                        "total": total_count,
                        "progress_pct": round(progress, 1),
                        "table_name": config.table_name
                    }
                )

            # Mark as completed
            metrics.mark_completed()
            summary = metrics.get_summary_message()
            self.logger.info(
                "Data transfer completed successfully",
                extra={
                    "table_name": config.table_name,
                    "records_transferred": metrics.transferred_records,
                    "batch_count": metrics.batch_count,
                    "duration_seconds": metrics.duration_seconds,
                    "summary": summary
                }
            )

            return TransferResult.create_success(
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=total_count,
            )

        except (KafkaProducerError, DataReadError):
            # Re-raise specific exceptions
            raise
        except Exception as e:
            error_message = f"Transfer failed for {config.table_name}: {str(e)}"
            self.logger.error(
                "Data transfer failed",
                extra={
                    "table_name": config.table_name,
                    "execution_date": execution_date,
                    "error": str(e),
                    "records_transferred": metrics.transferred_records,
                    "batch_count": metrics.batch_count
                },
                exc_info=True
            )
            metrics.mark_failed(error_message)
            
            return TransferResult.create_failure(
                error_message=error_message,
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=metrics.total_records,
            )

        finally:
            # Cleanup resources
            if producer:
                try:
                    producer.close()
                except Exception as e:
                    self.logger.warning(f"Error closing producer: {e}")

    def transfer_query_data(
        self,
        config: QueryConfiguration,
        execution_date: str,
        batch_size: int,
        kafka_topic: Optional[str]=None,
        clickhouse_database : Optional[str]=None,
        clickhouse_table_name : Optional[str]=None,
    ) -> TransferResult:
        """
        Execute SQL query and transfer results to Kafka topic.

        Args:
            config: Query configuration with SQL and Kafka settings
            execution_date: Execution date in YYYY-MM-DD format

        Returns:
            TransferResult with transfer statistics
        """
        # Initialize metrics
        metrics = TransferMetrics(
            table_name=config.source_name,
            execution_date=execution_date,
        )

        self.logger.info(
            "Starting query transfer",
            extra={
                "source_name": config.source_name,
                "kafka_topic": kafka_topic,
                "execution_date": execution_date,
                "batch_size": batch_size,
            }
        )

        # Initialize components
        producer = None
        reader = None
        clickhouse_writer = None

        try:
            # Initialize producer
            if self.is_send_kafka:
                client_id = f"airflow-query-{config.source_name.replace('.', '-').replace('_', '-')}"
                producer = IdempotentKafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap_servers,
                    client_id=client_id,
                )
                
            if self.is_send_clickhouse:
                clickhouse_writer = ClickHouseWriter(self.clickhouse_conn_id)
            
            # Initialize reader
            reader = MSSQLDataReader(
                conn_id=self.mssql_conn_id,
                batch_size=batch_size,
                is_connection_string=self.is_connection_string,
            )
            
            # Get total count for metrics
            if config.count_query is not None:
                total_count = reader.get_total_count(
                    custom_query=config.count_query,
                    params=config.query_params
                )
                metrics.total_records = total_count

                self.logger.info(
                    "Total records to transfer",
                    extra={
                        "source_name": config.source_name,
                        "total_count": total_count,
                        "execution_date": execution_date
                    }
                )

                if total_count == 0:
                    self.logger.info(
                        "No records to transfer",
                        extra={"source_name": config.source_name, "execution_date": execution_date}
                    )
                    metrics.mark_completed()
                    return TransferResult.create_success(
                        records_transferred=0,
                        batch_count=0,
                        duration_seconds=metrics.duration_seconds,
                        total_records=total_count,
                    )
                        
            # Stream and transfer data
            batch_number = 0
            for batch in reader.stream_query(
                query= config.query,
                count_query= config.count_query,
                parameters= config.query_params
            ):                   
                batch_number += 1
                batch_size = len(batch)

                # Send batch
                if self.is_send_kafka and producer:
                    try:
                        producer.send_batch_to_kafka(
                            batch=batch,
                            topic=kafka_topic,
                            source_name=config.source_name,
                            key_column=config.key_column,
                            execution_date=execution_date,
                            batch_number=batch_number,
                            version=self.version
                        )
                        self.logger.debug(f"Batch {batch_number} flushed to Kafka ({len(batch)} records)")
                    except Exception as e:
                        self.logger.error(f"Kafka send failed: {e}")
                        if self.fail_on_error:
                            raise
                                        
                if self.is_send_clickhouse and clickhouse_writer:
                    try:
                        clickhouse_writer.upsert_batch(clickhouse_database, clickhouse_table_name, batch, self.version)
                        self.logger.debug(f"Batch {batch_number} Inserted to clickhouse ({len(batch)} records)")
                    except Exception as e:
                        self.logger.error(f"ClickHouse write failed: {e}")
                        if self.fail_on_error:
                            raise
                        
                # Update metrics
                metrics.increment_batch(batch_size)

                # Log progress
                progress = metrics.get_progress_percentage()
                self.logger.info(
                    "Batch transferred",
                    extra={
                        "batch_number": batch_number,
                        "batch_size": batch_size,
                        "transferred": metrics.transferred_records,
                        "total": total_count if config.count_query is not None else None,
                        "progress_pct": round(progress, 1),
                        "source_name": config.source_name
                    }
                )    

            # Mark as completed
            metrics.mark_completed()
            summary = metrics.get_summary_message()
            self.logger.info(
                "Query transfer completed successfully",
                extra={
                    "source_name": config.source_name,
                    "records_transferred": metrics.transferred_records,
                    "batch_count": metrics.batch_count,
                    "duration_seconds": metrics.duration_seconds,
                    "summary": summary
                }
            )

            return TransferResult.create_success(
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=total_count if config.count_query is not None else None,
            )

        except (KafkaProducerError, DataReadError):
            raise
        except Exception as e:
            error_message = f"Query transfer failed for {config.source_name}: {str(e)}"
            self.logger.error(
                "Query transfer failed",
                extra={
                    "source_name": config.source_name,
                    "execution_date": execution_date,
                    "error": str(e),
                    "records_transferred": metrics.transferred_records,
                    "batch_count": metrics.batch_count,
                },
                exc_info=True
            )
            metrics.mark_failed(error_message)

            return TransferResult.create_failure(
                error_message=error_message,
                records_transferred=metrics.transferred_records,
                batch_count=metrics.batch_count,
                duration_seconds=metrics.duration_seconds,
                total_records=metrics.total_records,
            )

        finally:
            if producer:
                try:
                    producer.close()
                except Exception as e:
                    self.logger.warning(f"Error closing producer: {e}")