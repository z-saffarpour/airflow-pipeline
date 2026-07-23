from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore
from airflow.models.param import Param # type: ignore 
from airflow.models import Variable # type: ignore


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': "Zahra Saffarpour",
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['Saffarpour.Zahra@okco.ir'],
    'retries': int(Variable.get("retries_ax_retail_discount_line_sync_orchestrator", default_var=1)),
    'retry_delay': timedelta(minutes=int(Variable.get("retry_delay_minutes_ax_retail_discount_line_sync_orchestrator", default_var=5))),
    'execution_timeout': timedelta(hours=int(Variable.get("execution_timeout_hours_ax_retail_discount_line_sync_orchestrator", default_var=8))),
    'pool': "default_pool",
}

with DAG(
    dag_id="ax_retail_discount_line_sync_orchestrator",
    description="Orchestrates AX retail discount line synchronization DAGs for a given store.",
    default_args=default_args,
    start_date = datetime(2026, 5, 12),
    schedule = None,
    catchup = False, # No backfill for dimension tables
    max_active_runs = 8,
    tags=["mssql","store", "master-data", "orchestrator"],
    params={
        'store_number': Param(
            default='OKS00000',
            type='string',
            minLength=8,
            maxLength=8,
            description="Enter the store number for synchronization",
            title='Store Number'
        ),
    },
) as dag:
    trigger_ax_retail_periodic_discount_line = TriggerDagRunOperator(
        task_id='ax_retail_periodic_discount_line',
        trigger_dag_id='ax_retail_periodic_discount_line_sync',
        conf={"store_number": "{{ params.store_number }}"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    trigger_ax_retail_discount_line_offer = TriggerDagRunOperator(
        task_id='ax_retail_discount_line_offer',
        trigger_dag_id='ax_retail_discount_line_offer_sync',
        conf={"store_number": "{{ params.store_number }}"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    trigger_ax_retail_discount_line_mix_and_match = TriggerDagRunOperator(
        task_id='ax_retail_discount_line_mix_and_match',
        trigger_dag_id='ax_retail_discount_line_mix_and_match_sync',
        conf={"store_number": "{{ params.store_number }}"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    trigger_ax_retail_mix_and_match_line_groups = TriggerDagRunOperator(
        task_id='ax_retail_mix_and_match_line_groups',
        trigger_dag_id='ax_retail_mix_and_match_line_groups_sync',
        conf={"store_number": "{{ params.store_number }}"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    trigger_ax_retail_discount_line_multibuy = TriggerDagRunOperator(
        task_id='ax_retail_discount_line_multibuy',
        trigger_dag_id='ax_retail_discount_line_multibuy_sync',
        conf={"store_number": "{{ params.store_number }}"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    trigger_ax_retail_multibuy_discount_line = TriggerDagRunOperator(
        task_id='ax_retail_multibuy_discount_line',
        trigger_dag_id='ax_retail_multibuy_discount_line_sync',
        conf={"store_number": "{{ params.store_number }}"},
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=['failed'],
    )
    
    trigger_ax_retail_periodic_discount_line >> [
        trigger_ax_retail_discount_line_offer,
        trigger_ax_retail_discount_line_mix_and_match,
        trigger_ax_retail_mix_and_match_line_groups,
        trigger_ax_retail_discount_line_multibuy,
        trigger_ax_retail_multibuy_discount_line
    ]