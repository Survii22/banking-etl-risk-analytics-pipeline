"""
banking_etl_dag.py
-------------------
Phase 5 — Apache Airflow Orchestration

Orchestrates the full Banking ETL & Risk Analytics pipeline end to end:

    start
      |
    extract_data          (Phase 0: generate/refresh synthetic source files)
      |
    validate_data         (Phase 1: ingest.py — schema validation + landing)
      |
    pyspark_transform      (Phase 2: pyspark_etl.py — clean/join/aggregate)
      |
    quality_check          (Phase 3: data_quality.py — DQ gate)
      |
    load_to_sql            (Phase 4: load_to_sql.py — star-schema warehouse)
      |
    pipeline_success        (notify / mark run complete)

Design notes for a Deloitte-style production pipeline:
  - Each task shells out to the existing, independently-runnable scripts
    (via BashOperator) rather than re-implementing their logic inline. This
    keeps the DAG a thin orchestration layer and the scripts unit-testable
    and runnable standalone (as demonstrated in this project).
  - `quality_check` is a hard gate: if the data-quality score falls below
    the threshold in quality/data_quality.py, that task fails (non-zero
    exit code) and `load_to_sql` is never triggered — bad data never
    reaches the warehouse.
  - Retries + retry_delay simulate resilience against transient failures
    (e.g. a flaky source API in a real deployment).
  - `on_failure_callback` hooks are stubbed with logging.error() calls;
    in production these would page/alert (Slack, PagerDuty, email).
  - Schedule is daily (`@daily`) to mirror a nightly banking batch window;
    change `schedule_interval` for a different cadence.

To run against a local Airflow (not required for this portfolio project's
core deliverable, but included so the orchestration layer is complete):
    1. pip install apache-airflow==2.9.*
    2. export AIRFLOW_HOME=~/airflow && airflow db init
    3. Copy this file into $AIRFLOW_HOME/dags/
    4. airflow webserver -p 8080  &  airflow scheduler
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

PROJECT_ROOT = "/opt/airflow/banking_etl_pipeline"  # mount point in the Airflow container/host
PYTHON_BIN = "python3"

default_args = {
    "owner": "risk_technology_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def _on_failure_alert(context):
    """Stub alert hook — wire this up to Slack/PagerDuty/email in production."""
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    logging.error(f"[ALERT] Task '{task_id}' in DAG '{dag_id}' FAILED. Investigate immediately.")


def _pipeline_success_callable(**context):
    logging.info("Banking ETL & Risk Analytics pipeline completed successfully.")
    logging.info(f"Run date: {context['ds']}")
    logging.info("Warehouse is up to date. Power BI dashboards can now refresh.")


with DAG(
    dag_id="banking_etl_risk_analytics_pipeline",
    description="End-to-end Banking ETL and Risk Analytics pipeline (ingestion -> PySpark ETL -> DQ -> SQL warehouse)",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "risk", "etl", "deloitte-portfolio"],
    on_failure_callback=_on_failure_alert,
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command='echo "Starting Banking ETL & Risk Analytics pipeline run for {{ ds }}"',
    )

    # --- Extract: (re)generate/refresh the source CSVs -----------------------
    # In production this would poll an SFTP drop, hit a core-banking API, or
    # read from a Kafka topic. Here it (re)runs the synthetic generator so
    # the demo pipeline is fully self-contained and repeatable.
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command=(
            f"cd {PROJECT_ROOT}/data_generator && "
            f"{PYTHON_BIN} generate_synthetic_data.py "
            f"--num_customers 5000 --num_accounts 8000 "
            f"--num_transactions 200000 --num_loans 3000"
        ),
        on_failure_callback=_on_failure_alert,
    )

    # --- Validate: Phase 1 ingestion (schema validation + landing zone) -----
    validate_data = BashOperator(
        task_id="validate_data",
        bash_command=f"cd {PROJECT_ROOT}/ingestion && {PYTHON_BIN} ingest.py",
        on_failure_callback=_on_failure_alert,
    )

    # --- Transform: Phase 2 PySpark ETL (clean/join/aggregate/risk score) ---
    pyspark_transform = BashOperator(
        task_id="pyspark_transform",
        bash_command=f"cd {PROJECT_ROOT}/etl && {PYTHON_BIN} pyspark_etl.py",
        execution_timeout=timedelta(minutes=45),  # Spark jobs get more headroom
        on_failure_callback=_on_failure_alert,
    )

    # --- Quality gate: Phase 3 automated DQ checks ---------------------------
    # Exits non-zero if data_quality_score falls below QUALITY_GATE_THRESHOLD,
    # which fails this task and halts the DAG before load_to_sql runs.
    quality_check = BashOperator(
        task_id="quality_check",
        bash_command=f"cd {PROJECT_ROOT}/quality && {PYTHON_BIN} data_quality.py",
        on_failure_callback=_on_failure_alert,
    )

    # --- Load: Phase 4 SQL warehouse load (star schema) ----------------------
    load_to_sql = BashOperator(
        task_id="load_to_sql",
        bash_command=f"cd {PROJECT_ROOT}/sql && {PYTHON_BIN} load_to_sql.py",
        on_failure_callback=_on_failure_alert,
    )

    pipeline_success = PythonOperator(
        task_id="pipeline_success",
        python_callable=_pipeline_success_callable,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    start >> extract_data >> validate_data >> pyspark_transform >> quality_check >> load_to_sql >> pipeline_success
