"""
Regulatory Reporting DAG
Generates CBUAE-compliant regulatory reports on a monthly schedule.

Schedule: First day of each month
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "compliance-team",
    "depends_on_past": True,
    "email": ["compliance@lakehouse.local"],
    "email_on_failure": True,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=4),
}


def validate_regulatory_data(**context):
    """
    Pre-validate Gold layer data before generating regulatory reports.
    Ensures data completeness and compliance requirements are met.
    """
    import sys
    sys.path.insert(0, "/opt/spark-jobs")
    from utils.spark_session import create_spark_session
    from utils.delta_helpers import read_delta

    spark = create_spark_session(app_name="RegulatoryValidation")

    # Validate Customer 360 exists and has data
    c360 = read_delta(spark, "s3a://gold/customer_360")
    c360_count = c360.count()

    # Validate regulatory report exists
    reg = read_delta(spark, "s3a://gold/regulatory_reports")
    reg_count = reg.count()

    spark.stop()

    if c360_count == 0 or reg_count == 0:
        raise ValueError(
            f"Regulatory data incomplete: "
            f"customer_360={c360_count}, regulatory_reports={reg_count}"
        )

    print(f"✅ Regulatory data validated: "
          f"customer_360={c360_count}, regulatory_reports={reg_count}")


def export_regulatory_report(**context):
    """
    Export regulatory report to a date-stamped archive for CBUAE submission.
    """
    import sys
    sys.path.insert(0, "/opt/spark-jobs")
    from utils.spark_session import create_spark_session
    from utils.delta_helpers import read_delta

    spark = create_spark_session(app_name="RegulatoryExport")

    report_df = read_delta(spark, "s3a://gold/regulatory_reports")

    # Export as CSV for regulatory submission
    execution_date = context["ds"]
    export_path = f"s3a://gold/exports/cbuae_report_{execution_date}"

    report_df.coalesce(1).write.mode("overwrite").option(
        "header", "true"
    ).csv(export_path)

    record_count = report_df.count()
    spark.stop()

    print(f"✅ CBUAE Report exported: {record_count} records → {export_path}")
    return {"path": export_path, "record_count": record_count}


with DAG(
    dag_id="regulatory_reporting",
    default_args=default_args,
    description="CBUAE regulatory report generation and export",
    schedule_interval="0 2 1 * *",  # 2 AM on 1st of each month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["regulatory", "cbuae", "compliance"],
    doc_md="""
    ## CBUAE Regulatory Reporting

    Monthly regulatory report generation and export.
    Runs on the 1st of each month at 2:00 AM UTC.

    ### Requirements:
    - Gold layer must be up-to-date
    - Customer 360 and regulatory_reports tables must be populated
    - Data residency: UAE
    """,
) as dag:

    # Refresh Gold layer data
    refresh_gold = BashOperator(
        task_id="refresh_gold_models",
        bash_command=(
            "cd /opt/dbt-project && "
            "dbt run --models gold.regulatory_report --profiles-dir . --target dev --full-refresh"
        ),
    )

    # Validate data completeness
    validate_data = PythonOperator(
        task_id="validate_regulatory_data",
        python_callable=validate_regulatory_data,
    )

    # Test Gold models
    test_gold = BashOperator(
        task_id="test_gold_models",
        bash_command=(
            "cd /opt/dbt-project && "
            "dbt test --models gold.regulatory_report --profiles-dir . --target dev"
        ),
    )

    # Export for CBUAE submission
    export_report = PythonOperator(
        task_id="export_regulatory_report",
        python_callable=export_regulatory_report,
    )

    refresh_gold >> validate_data >> test_gold >> export_report
