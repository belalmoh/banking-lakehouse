"""
Data Quality Monitoring DAG
Runs comprehensive data quality checks across all lakehouse layers.

Schedule: Every 6 hours
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["alerts@lakehouse.local"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def check_bronze_layer(**context):
    """Run quality checks on all Bronze tables."""
    import sys
    sys.path.insert(0, "/opt/spark-jobs")
    from data_quality_checks import validate_bronze_transactions
    from utils.spark_session import create_spark_session

    spark = create_spark_session(app_name="DQ-Bronze")
    result = validate_bronze_transactions(spark, "s3a://bronze")
    spark.stop()

    context["ti"].xcom_push(key="bronze_dq_result", value=result)
    return result


def check_silver_layer(**context):
    """Run quality checks on all Silver tables."""
    import sys
    sys.path.insert(0, "/opt/spark-jobs")
    from data_quality_checks import validate_silver_transactions
    from utils.spark_session import create_spark_session

    spark = create_spark_session(app_name="DQ-Silver")
    result = validate_silver_transactions(spark, "s3a://silver")
    spark.stop()

    context["ti"].xcom_push(key="silver_dq_result", value=result)
    return result


def check_gold_layer(**context):
    """Run quality checks on all Gold tables."""
    import sys
    sys.path.insert(0, "/opt/spark-jobs")
    from data_quality_checks import validate_gold_customer_360
    from utils.spark_session import create_spark_session

    spark = create_spark_session(app_name="DQ-Gold")
    result = validate_gold_customer_360(spark, "s3a://gold")
    spark.stop()

    context["ti"].xcom_push(key="gold_dq_result", value=result)
    return result


def generate_dq_report(**context):
    """Aggregate and report quality check results."""
    ti = context["ti"]
    bronze = ti.xcom_pull(task_ids="check_bronze_layer", key="bronze_dq_result")
    silver = ti.xcom_pull(task_ids="check_silver_layer", key="silver_dq_result")
    gold = ti.xcom_pull(task_ids="check_gold_layer", key="gold_dq_result")

    report = {
        "timestamp": datetime.utcnow().isoformat(),
        "bronze_passed": bronze.get("passed", False) if bronze else False,
        "silver_passed": silver.get("passed", False) if silver else False,
        "gold_passed": gold.get("passed", False) if gold else False,
    }
    report["all_passed"] = all([
        report["bronze_passed"],
        report["silver_passed"],
        report["gold_passed"],
    ])

    print(f"\n{'='*60}")
    print(f"DATA QUALITY REPORT - {report['timestamp']}")
    print(f"{'='*60}")
    print(f"  Bronze: {'✅ PASS' if report['bronze_passed'] else '❌ FAIL'}")
    print(f"  Silver: {'✅ PASS' if report['silver_passed'] else '❌ FAIL'}")
    print(f"  Gold:   {'✅ PASS' if report['gold_passed'] else '❌ FAIL'}")
    print(f"{'─'*60}")
    print(f"  Overall: {'✅ ALL PASSED' if report['all_passed'] else '❌ ISSUES FOUND'}")
    print(f"{'='*60}\n")

    return report


with DAG(
    dag_id="data_quality_monitoring",
    default_args=default_args,
    description="Comprehensive data quality monitoring across all lakehouse layers",
    schedule_interval="0 */6 * * *",  # Every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["quality", "monitoring", "lakehouse"],
) as dag:

    bronze_check = PythonOperator(
        task_id="check_bronze_layer",
        python_callable=check_bronze_layer,
    )

    silver_check = PythonOperator(
        task_id="check_silver_layer",
        python_callable=check_silver_layer,
    )

    gold_check = PythonOperator(
        task_id="check_gold_layer",
        python_callable=check_gold_layer,
    )

    dq_report = PythonOperator(
        task_id="generate_dq_report",
        python_callable=generate_dq_report,
    )

    [bronze_check, silver_check, gold_check] >> dq_report
