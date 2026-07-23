from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from airflow.sensors.time_delta import TimeDeltaSensor # type: ignore


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': "Zahra Saffarpour",
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['Saffarpour.Zahra@okco.ir'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=8),
    'pool': "default_pool",
}

with DAG(
    dag_id="query_inventory_and_sales_sync_orchestrator",
    description="Hourly sync of retail store, e-commerce, sales order, and on-hand inventory data to Kafka.",
    default_args=default_args,
    start_date=datetime(2026, 4, 2),
    schedule="3 * * * *",            # Hourly at 2:00 AM
    catchup=False,                   # Don't backfill historical runs
    max_active_runs=1,               # Prevent overlapping executions
    tags=["mssql", "kafka", "inventory", "orchestrator"],
) as dag:       
    with TaskGroup("mssql_sync") as mssql_sync:
        trigger_onhand_lite_sync = TriggerDagRunOperator(
            task_id='onhand_lite_sync',
            trigger_dag_id='query_inventory_onhand_lite_sync',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )
        trigger_purch_sync = TriggerDagRunOperator(
            task_id='purch_sync',
            trigger_dag_id='query_inventory_purch_sync',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )
        trigger_sales_online_sync = TriggerDagRunOperator(
            task_id='sales_online_sync',
            trigger_dag_id='query_inventory_sales_online_sync',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )
        trigger_sales_order_sync = TriggerDagRunOperator(
            task_id='sales_order_sync',
            trigger_dag_id='query_inventory_sales_order_sync',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )
        trigger_sales_retail_sync = TriggerDagRunOperator(
            task_id='sales_retail_sync',
            trigger_dag_id='query_inventory_sales_retail_sync',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )
        
        # Internal dependencies
        trigger_sales_retail_sync
        trigger_sales_order_sync
        trigger_onhand_lite_sync
        trigger_sales_online_sync >> trigger_purch_sync
        
    with TaskGroup("clickhouse") as clickhouse:
        inventory_onhand_processing_clickhouse = TriggerDagRunOperator(
            task_id='inventory_onhand_processing_clickhouse',
            trigger_dag_id='inventory_onhand_processing_clickhouse',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )
        
        inventory_onhand_optimizer = TriggerDagRunOperator(
            task_id='inventory_onhand',
            trigger_dag_id='inventory_onhand_clickhouse_optimizer',
            reset_dag_run=True,
            wait_for_completion=True,
            poke_interval=60,
            allowed_states=['success'],
            failed_states=['failed'],
        )
        
        # Internal dependencies
        inventory_onhand_processing_clickhouse >> inventory_onhand_optimizer
        
        
    wait_2_min = TimeDeltaSensor(
        task_id="wait_2_min",
        delta=timedelta(minutes=2)
    )
        
    mssql_sync >> wait_2_min >> clickhouse