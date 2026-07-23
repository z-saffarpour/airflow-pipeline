"""
Airflow DAG: ClickHouse Table Optimization Pipeline (TEMPLATE)
===============================================================
This is a TEMPLATE DAG for ClickHouse table optimization.
Do NOT run this DAG directly - create specific DAGs for each table.

Features:
- Partition-based optimization
- FINAL merge support
- Deduplication support
- Health check before/after optimization
- Configurable parameters

Usage:
    Copy this template to create table-specific DAGs:
    - dim_date_clickhouse_optimizer.py
    - fact_sales_trans_clickhouse_optimizer.py

Author: Senior Data Engineer
Version: 2.0
"""

from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from airflow.exceptions import AirflowException  # type: ignore
from airflow.utils.log.logging_mixin import LoggingMixin  # type: ignore

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.ClickHouseOptimizationConfig import ClickHouseOptimizationConfig
from pipeline.core.ClickHouseOptimizationOrchestrator import ClickHouseOptimizationOrchestrator
from pipeline.core.ExecutionDateExtractor import ExecutionDateExtractor

from pipeline.utils.validation import validate_clickhouse_conn

# ============================================================================
# LOGGING
# ============================================================================

logger = LoggingMixin().log

# ============================================================================
# TASK FACTORIES
# ============================================================================

def make_validate_clickhouse_task(clickhouse_conn_id: str):
    """Factory: validate ClickHouse connection task."""
    @task(
        task_id="validate_clickhouse_connection",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def validate_clickhouse_connection():
        """
        Validate ClickHouse connection.
        """
        validate_clickhouse_conn(clickhouse_conn_id)

    return validate_clickhouse_connection

def make_check_table_health_before_task(clickhouse_conn_id: str, config: ClickHouseOptimizationConfig):
    """Factory: check_table_health_before task."""
    @task(
        task_id="check_table_health_before",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def check_table_health_before():
        """
        Check table health before optimization.
        """    
        try:            
            # Create orchestrator and run optimization
            orchestrator = ClickHouseOptimizationOrchestrator(conn_id=clickhouse_conn_id)
        
            # Run 
            result = orchestrator.check_table_health(config.database, config.table_name)
            
            # Raise exception if optimization failed
            if result["status"] == 'failed':
                raise AirflowException(f"Optimization failed: {result.error_message}")
            return result
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}", exc_info=True)
            raise AirflowException(f"Health check failed: {str(e)}")

    return check_table_health_before

def make_run_optimization_task(clickhouse_conn_id: str, config: ClickHouseOptimizationConfig):
    """Factory: run_optimization task."""
    @task(
        task_id="run_optimization",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def run_optimization(**context):
        """
        Run OPTIMIZE TABLE on ClickHouse.
        """        
        # Get execution date
        execution_date = ExecutionDateExtractor.get_date_key_from_context(context)
        
        logger.info(f"Starting optimization for {config.full_table_name}")
        logger.info(f"Execution date: {execution_date}")
        logger.info(f"Final: {config.final}, Deduplicate: {config.deduplicate}")
        
        # Create orchestrator and run optimization
        orchestrator = ClickHouseOptimizationOrchestrator(conn_id=clickhouse_conn_id)
        
        # Run optimization
        result = orchestrator.optimize_table(
            config=config,
            execution_date=execution_date,
        )
        
        # Log result
        logger.info(result.get_summary())
        
        # Raise exception if optimization failed
        if result.is_failure:
            raise AirflowException(f"Optimization failed: {result.error_message}")
        
        return {
            'execution_date': execution_date,
            'table_name': result.table_name,
            'partition': result.partition,
            'status': result.status,
            'parts_before': result.parts_before,
            'parts_after': result.parts_after,
            'parts_merged': result.parts_merged,
            'duration_seconds': result.duration_seconds,
        }
        
    return run_optimization


def make_check_table_health_after_task(clickhouse_conn_id: str, config: ClickHouseOptimizationConfig):
    """Factory: check_table_health_after task."""
    @task(
        task_id="check_table_health_after",
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def check_table_health_after(**context):
        """
        Check table health after optimization and verify improvement.
        """        
        # Get before stats from previous task
        before_stats = context['ti'].xcom_pull(task_ids='health_checks.check_table_health_before')
        optimization_result = context['ti'].xcom_pull(task_ids='optimization.run_optimization')
        
        try:       
            # Create orchestrator and run optimization
            orchestrator = ClickHouseOptimizationOrchestrator(conn_id=clickhouse_conn_id)
        
            # Run 
            result = orchestrator.check_table_health(config.database, config.table_name)
            
            # Raise exception if optimization failed
            if result["status"] == 'failed':
                raise AirflowException(f"Optimization failed: {result.error_message}")
            
            # Calculate improvements
            parts_before = before_stats.get('parts_count', 0)
            parts_after = result.get('parts_count',0)
            parts_reduced = parts_before - parts_after
            
            logger.info(f"Health after optimization: {result["health_status"]}")
            logger.info(f"Parts reduced: {parts_before} -> {parts_after} ({parts_reduced} merged)")
            
            return {
                'table_name': result["table_name"],
                'health_status': result["health_status"],
                'parts_before': parts_before,
                'parts_after': parts_after,
                'parts_reduced': parts_reduced,
                'total_rows': result["total_rows"],
                'bytes_on_disk': result["bytes_on_disk"],
                'optimization_duration': optimization_result.get('duration_seconds', 0),
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Post-optimization health check failed: {str(e)}", exc_info=True)
            raise AirflowException(f"Post-optimization check failed: {str(e)}")

    return check_table_health_after

# ============================================================================
# DAG FACTORY
# ============================================================================
def clickhouse_optimizer_dag(
    dag_config: DAGConfig,
    conn_config: ConnectionConfig,
    optimize_config: ClickHouseOptimizationConfig,
):
    """
    TEMPLATE DAG for ClickHouse table optimization.
    
    WARNING: This is a template DAG and should NOT run automatically.
    Create specific DAGs for each table by importing tasks from this file.
    
    Flow:
    1. Validate ClickHouse Connection
    2. Check Table Health (before)
    3. Run OPTIMIZE TABLE
    4. Check Table Health (after)
    
    Example Usage:
        from pipeline.config import DAGConfig, ConnectionConfig, ClickHouseOptimizationConfig
        from template.clickhouse_optimizer_dag_factory import clickhouse_optimizer_dag

        conn_config = ConnectionConfig(clickhouse_conn_id='clickhouse_default')
        clickhouse_optimizer_dag(DAG_CONFIG, conn_config, OPTIMIZE_CONFIG)
    """
    clickhouse_conn_id = conn_config.clickhouse_conn_id
    if not clickhouse_conn_id:
        raise ValueError(
            "ConnectionConfig.clickhouse_conn_id is required for clickhouse_optimizer_dag"
        )

    default_args = {
        'owner': dag_config.owner,
        'depends_on_past': dag_config.depends_on_past,
        'email_on_failure': True,
        'email_on_retry': False,
        'email': ['Saffarpour.Zahra@okco.ir'],
        'retries': dag_config.retries,
        'retry_delay': dag_config.retry_delay,
        'execution_timeout': dag_config.execution_timeout,
        'pool': dag_config.pool,
        # 'priority_weight': 5,
    }

    with DAG(
        dag_id=dag_config.dag_id,
        default_args=default_args,
        description=dag_config.description,
        schedule=dag_config.schedule,  # Template DAG - should not run automatically
        start_date=dag_config.start_date,
        catchup=dag_config.catchup,
        is_paused_upon_creation=dag_config.is_paused_upon_creation,  # Disabled by default
        max_active_runs=dag_config.max_active_runs,
        max_active_tasks=dag_config.max_active_tasks,
        tags=dag_config.tags,
        doc_md=__doc__,
    ) as dag:
        
        # Validate connection
        validate_conn = make_validate_clickhouse_task(clickhouse_conn_id=clickhouse_conn_id)()
        
        with TaskGroup(group_id='health_checks') as health_group:
            health_before = make_check_table_health_before_task(clickhouse_conn_id=clickhouse_conn_id, config=optimize_config)()
            
            health_before
        
        with TaskGroup(group_id='optimization') as opt_group:
            optimize = make_run_optimization_task(clickhouse_conn_id=clickhouse_conn_id, config=optimize_config)()
            
            optimize
        
        health_after = make_check_table_health_after_task(clickhouse_conn_id=clickhouse_conn_id, config=optimize_config)()
        
        # Task dependencies
        validate_conn >> health_group >> opt_group >> health_after

    return dag