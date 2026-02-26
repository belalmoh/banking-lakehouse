"""
Banking Lakehouse Pipeline DAG
Main orchestration DAG for the Bronze → Silver → Gold data pipeline.

Schedule: Hourly
Retries: 3 attempts, 5-min delay
SLA: 2 hours
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


# ─── DAG Default Arguments ──────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["alerts@lakehouse.local"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "sla": timedelta(hours=2),
}


# ─── DAG Definition ─────────────────────────────────────────────────────────

with DAG(
    dag_id="banking_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-end Banking Lakehouse pipeline: Bronze → Silver → Gold",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["banking", "lakehouse", "production"],
    doc_md="""
    ## Banking Lakehouse Pipeline

    **Owner:** Data Engineering Team
    **Schedule:** Hourly
    **SLA:** 2 hours

    ### Pipeline Steps:
    1. **Bronze Ingestion** — Load raw data via Spark
    2. **Data Quality (Bronze)** — Validate ingested data
    3. **Silver Transformations** — dbt cleansing models
    4. **Gold Aggregations** — dbt business models
    5. **dbt Tests** — Run dbt test suite
    6. **Post-Pipeline Checks** — Final validation
    """,
) as dag:

    # ─── Task 1: Bronze Ingestion ────────────────────────────────────────────

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command=(
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--driver-memory 2g "
            "--executor-memory 2g "
            "--packages io.delta:delta-core_2.12:2.4.0 "
            "/opt/spark-jobs/bronze_ingestion.py"
        ),
        doc_md="Ingest raw CSV/JSON banking data into Bronze Delta tables.",
    )

    # ─── Task 2: Data Quality Checks (Bronze) ───────────────────────────────

    def run_bronze_quality_checks(**context):
        """Execute data quality validation on Bronze layer."""
        import sys
        sys.path.insert(0, "/opt/spark-jobs")
        from data_quality_checks import validate_bronze_transactions
        from utils.spark_session import create_spark_session

        spark = create_spark_session(app_name="BronzeQualityCheck")
        result = validate_bronze_transactions(spark, "s3a://bronze")
        spark.stop()

        if not result.get("passed", False):
            raise ValueError(
                f"Bronze quality checks FAILED: {result}"
            )
        context["ti"].xcom_push(key="bronze_quality", value=result)

    bronze_quality = PythonOperator(
        task_id="bronze_data_quality",
        python_callable=run_bronze_quality_checks,
        doc_md="Validate Bronze data: nulls, uniqueness, value ranges.",
    )

    # ─── Task 3: dbt Silver Transformations ──────────────────────────────────

    with TaskGroup(group_id="silver_transformations") as silver_group:
        dbt_silver_run = BashOperator(
            task_id="dbt_run_silver",
            bash_command=(
                "cd /opt/dbt-project && "
                "dbt run --models silver.* --profiles-dir . --target dev"
            ),
            doc_md="Run dbt Silver layer models (incremental cleansing).",
        )

        dbt_silver_test = BashOperator(
            task_id="dbt_test_silver",
            bash_command=(
                "cd /opt/dbt-project && "
                "dbt test --models silver.* --profiles-dir . --target dev"
            ),
            doc_md="Run dbt tests on Silver models.",
        )

        dbt_silver_run >> dbt_silver_test

    # ─── Task 4: dbt Gold Aggregations ───────────────────────────────────────

    with TaskGroup(group_id="gold_aggregations") as gold_group:
        dbt_gold_run = BashOperator(
            task_id="dbt_run_gold",
            bash_command=(
                "cd /opt/dbt-project && "
                "dbt run --models gold.* --profiles-dir . --target dev"
            ),
            doc_md="Run dbt Gold layer models (full refresh aggregations).",
        )

        dbt_gold_test = BashOperator(
            task_id="dbt_test_gold",
            bash_command=(
                "cd /opt/dbt-project && "
                "dbt test --models gold.* --profiles-dir . --target dev"
            ),
            doc_md="Run dbt tests on Gold models.",
        )

        dbt_gold_run >> dbt_gold_test

    # ─── Task 5: Full dbt Test Suite ─────────────────────────────────────────

    dbt_test_all = BashOperator(
        task_id="dbt_test_all",
        bash_command=(
            "cd /opt/dbt-project && "
            "dbt test --profiles-dir . --target dev"
        ),
        doc_md="Run the complete dbt test suite across all models.",
    )

    # ─── Task 6: Post-Pipeline Quality Gate ──────────────────────────────────

    def run_final_quality_gate(**context):
        """Final quality validation on Gold layer."""
        import sys
        sys.path.insert(0, "/opt/spark-jobs")
        from data_quality_checks import validate_gold_customer_360
        from utils.spark_session import create_spark_session

        spark = create_spark_session(app_name="GoldQualityGate")
        result = validate_gold_customer_360(spark, "s3a://gold")
        spark.stop()

        if not result.get("passed", False):
            raise ValueError(f"Gold quality gate FAILED: {result}")

    final_quality_gate = PythonOperator(
        task_id="final_quality_gate",
        python_callable=run_final_quality_gate,
        doc_md="Validate Gold Customer 360 data integrity.",
    )

    # ─── Task Dependencies ───────────────────────────────────────────────────
    # Sequential with quality gates between layers

    bronze_ingestion >> bronze_quality >> silver_group >> gold_group >> dbt_test_all >> final_quality_gate
