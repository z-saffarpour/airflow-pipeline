"""
Airflow DAG: ClickHouse table optimization (example)
====================================================
Runs OPTIMIZE TABLE (+ optional FINAL / deduplicate) on a ClickHouse table,
with health checks before and after.

Uses clickhouse_optimizer_dag_factory.clickhouse_optimizer_dag.

Required Airflow connections:
  - clickhouse_default  — change as needed

Author: Zahra Saffarpour
Version: 1.0
"""
from datetime import datetime, timedelta
from airflow.models import Variable  # type: ignore

from pipeline.config.DAGConfig import DAGConfig
from pipeline.config.ConnectionConfig import ConnectionConfig
from pipeline.config.ClickHouseOptimizationConfig import ClickHouseOptimizationConfig

from template.clickhouse_optimizer_dag_factory import clickhouse_optimizer_dag

# ============================================================================
# CONFIGURATION
# ============================================================================

dag_config = DAGConfig(
    dag_id="example_table_clickhouse_optimizer",
    description="Example: optimize ClickHouse table after Kafka ingestion",
    owner="Zahra Saffarpour",
    start_date=datetime(2026, 7, 23),
    schedule=None,
    catchup=False,
    max_active_runs=int(
        Variable.get("max_active_runs_example_ch_optimizer", default_var=1)
    ),
    retries=int(Variable.get("retries_example_ch_optimizer", default_var=2)),
    retry_delay=timedelta(
        minutes=int(
            Variable.get("retry_delay_minutes_example_ch_optimizer", default_var=5)
        )
    ),
    execution_timeout=timedelta(
        hours=int(
            Variable.get("execution_timeout_hours_example_ch_optimizer", default_var=4)
        )
    ),
    tags=["clickhouse", "optimization", "example"],
    pool="clickhouse_optimizer_pool",
)

# Replace database / table / partition settings with your real ClickHouse target.
# For partitioned fact tables set partition_column and partition_format
# (e.g. YYYYMMDD or PERSIAN_YYYYMM).
optimize_config = ClickHouseOptimizationConfig(
    cluster_name=Variable.get(
        "clickhouse_cluster_name", default_var="cluster_2S_2R"
    ),
    database=Variable.get("example_ch_database", default_var="dbo"),
    table_name=Variable.get(
        "example_ch_table", default_var="Local_ExampleTable"
    ),
    partition_column=None,
    partition_format="YYYYMM",
    final=True,
    deduplicate=True,
)

conn_config = ConnectionConfig(
    clickhouse_conn_id=Variable.get(
        "clickhouse_conn_id", default_var="clickhouse_default"
    ),
)

# ============================================================================
# Create DAG from config
# ============================================================================

dag = clickhouse_optimizer_dag(dag_config, conn_config, optimize_config)

dag 