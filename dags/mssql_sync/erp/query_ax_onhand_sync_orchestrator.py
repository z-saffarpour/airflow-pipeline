from airflow import DAG # type: ignore
from airflow.sensors.time_delta import TimeDeltaSensor # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore

from datetime import datetime, timedelta

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': "Zahra Saffarpour",
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['Saffarpour.Zahra@okco.ir'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=8),
    'pool': "default_pool",
}

with DAG(
    dag_id="query_ax_onhand_sync_orchestrator",
    default_args = default_args,
    start_date=datetime(2026, 4, 5),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["mssql", "kafka", "ax", "orchestrator"],
) as dag:

    trigger_query_inventdim = TriggerDagRunOperator(
        task_id='invent_dim_sync',
        trigger_dag_id='query_ax_invent_dim_sync',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    trigger_invent_sum = TriggerDagRunOperator(
        task_id='invent_sum_sync',
        trigger_dag_id='query_ax_invent_sum_sync',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    wait_1_min = TimeDeltaSensor(
        task_id="wait_1_min",
        delta=timedelta(minutes=1)
    )

    trigger_whs_invent_reserve = TriggerDagRunOperator(
        task_id='whs_invent_reserve_full_sync',
        trigger_dag_id='query_ax_whs_invent_reserve_full_sync',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )

    [trigger_query_inventdim , trigger_invent_sum] >> wait_1_min >> trigger_whs_invent_reserve
