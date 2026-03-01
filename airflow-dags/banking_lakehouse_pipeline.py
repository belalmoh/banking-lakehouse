"""
Banking Lakehouse - End-to-End Pipeline (TaskFlow API)
============================================================
Orchestrates Bronze → Silver → Gold transformation with data quality gates

Interview talking points:
- "Using TaskFlow API for cleaner, more maintainable code"
- "PySpark tasks connect to Spark master over Docker network"
- "Automatic XCom passing between tasks with type hints"
- "Production-ready with SLA monitoring and quality gates"
"""

from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
import sys

# ============================================================================
# CONFIGURATION
# ============================================================================

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'email': ['data-eng@bank.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=2),
    'sla': timedelta(hours=4),
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

@dag(
    dag_id='banking_lakehouse_pipeline',
    default_args=DEFAULT_ARGS,
    description='ADCB Banking Lakehouse - Bronze → Silver → Gold Pipeline',
    schedule='0 2 * * *',  # Daily at 2 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['banking', 'lakehouse', 'production', 'taskflow'],
    doc_md=__doc__,
)
def banking_lakehouse_pipeline():
    """
    Main lakehouse pipeline DAG using TaskFlow API
    
    Interview point: "PySpark sessions connect to the Spark master over the 
    Docker network — no spark-submit binary needed in Airflow. Cleaner 
    separation of concerns with TaskFlow decorators."
    """
    
    # ========================================================================
    # TASK DEFINITIONS
    # ========================================================================
    
    @task(
        task_id='check_source_data',
        doc_md="Validates that source CSV files exist in MinIO before ingestion."
    )
    def check_source_data_available() -> List[str]:
        """Check if source data is available before starting pipeline"""
        import boto3
        from botocore.client import Config
        
        s3 = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='admin',
            aws_secret_access_key='admin123',
            config=Config(signature_version='s3v4')
        )
        
        required_files = [
            'banking/customers.csv',
            'banking/accounts.csv',
            'banking/transactions.csv',
            'banking/aml_alerts.csv'
        ]
        
        available_files = []
        missing_files = []
        
        for file_key in required_files:
            try:
                s3.head_object(Bucket='source', Key=file_key)
                logging.info(f"✅ Found: {file_key}")
                available_files.append(file_key)
            except Exception:
                missing_files.append(file_key)
                logging.error(f"❌ Missing: {file_key}")
        
        if missing_files:
            raise FileNotFoundError(
                f"Missing source files: {missing_files}. "
                f"Pipeline cannot proceed without all required data."
            )
        
        logging.info(f"✅ All {len(available_files)} source files available")
        return available_files
    
    @task(
        task_id='validate_bronze_counts',
        doc_md="Validates Bronze layer record counts as a data quality gate."
    )
    def validate_bronze_counts() -> Dict[str, Any]:
        """Validate Bronze layer record counts against expected minimums"""
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        builder = SparkSession.builder \
            .master("local[*]") \
            .appName("Validate-Bronze-Counts") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        extra_packages = [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "org.apache.hadoop:hadoop-client-runtime:3.3.4",
            "org.apache.hadoop:hadoop-client-api:3.3.4",
            "io.delta:delta-core_2.12:2.4.0",
            "io.delta:delta-storage:2.4.0",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
        spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
        spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
        
        expected_counts = {
            'customers': 10000,
            'accounts': 20000,
            'transactions': 100000,
            'aml_alerts': 5000
        }
        
        validation_results = {}
        all_valid = True
        
        for table_name, expected_min in expected_counts.items():
            actual_count = spark.read.format("delta").load(f"s3a://bronze/{table_name}").count()
            is_valid = actual_count >= expected_min
            validation_results[table_name] = {
                'expected_min': expected_min,
                'actual_count': actual_count,
                'valid': is_valid,
                'variance_pct': round((actual_count - expected_min) / expected_min * 100, 2)
            }
            
            status = "✅" if is_valid else "❌"
            logging.info(f"{status} {table_name}: {actual_count:,} (min: {expected_min:,})")
            if not is_valid:
                all_valid = False
        
        spark.stop()
        
        if not all_valid:
            raise ValueError(f"Bronze validation failed: {validation_results}")
        
        return validation_results
    
    @task.branch(task_id='data_quality_gate')
    def evaluate_data_quality(validation_results: Dict[str, Any]) -> str:
        """Branch: proceed to Silver if quality checks pass, else fail"""
        all_valid = all(r['valid'] for r in validation_results.values())
        if all_valid:
            logging.info("✅ Quality checks passed - proceeding to Silver")
            return 'proceed_to_silver'
        else:
            logging.error("❌ Quality checks failed - halting pipeline")
            return 'quality_failed'
    
    @task(task_id='publish_metrics')
    def publish_pipeline_metrics(validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Publish pipeline execution metrics"""
        metrics = {
            'pipeline_name': 'banking_lakehouse',
            'execution_timestamp': datetime.now().isoformat(),
            'status': 'SUCCESS',
            'bronze_validation': validation_results,
            'total_records_processed': sum(
                r['actual_count'] for r in validation_results.values()
            ),
        }
        logging.info(f"📊 Pipeline Metrics: {metrics}")
        return metrics
    
    # ========================================================================
    # BRONZE INGESTION (PySpark @task — connects to Spark master over network)
    # ========================================================================
    
    @task_group(group_id='bronze_ingestion')
    def bronze_ingestion_group():
        """
        Parallel Bronze layer ingestion using PySpark sessions
        
        Interview point: "Each task creates its own SparkSession connecting
        to the Spark master at spark://spark-master:7077. The driver runs
        inside Airflow, executors run on Spark workers. This gives us
        TaskFlow benefits (XCom, type hints) with distributed compute."
        """
        
        @task(task_id='ingest_customers', execution_timeout=timedelta(minutes=30))
        def ingest_customers():
            """Ingest customer data from source CSV to Bronze Delta Lake"""
            sys.path.insert(0, '/opt/spark-jobs/bronze')
            from ingest_customers import create_spark_session, ingest_to_bronze
            
            spark = create_spark_session("Airflow-Bronze-Customers")
            spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
            try:
                ingest_to_bronze(spark, "s3a://source/banking/customers.csv", "s3a://bronze/customers")
            finally:
                spark.stop()
        
        @task(task_id='ingest_accounts', execution_timeout=timedelta(minutes=30))
        def ingest_accounts():
            """Ingest account data from source CSV to Bronze Delta Lake"""
            sys.path.insert(0, '/opt/spark-jobs/bronze')
            from ingest_accounts import create_spark_session, ingest_to_bronze
            
            spark = create_spark_session("Airflow-Bronze-Accounts")
            spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
            try:
                ingest_to_bronze(spark, "s3a://source/banking/accounts.csv", "s3a://bronze/accounts")
            finally:
                spark.stop()
        
        @task(task_id='ingest_transactions', execution_timeout=timedelta(hours=1))
        def ingest_transactions():
            """Ingest transaction data from source CSV to Bronze Delta Lake"""
            sys.path.insert(0, '/opt/spark-jobs/bronze')
            from ingest_transactions import create_spark_session, ingest_to_bronze
            
            spark = create_spark_session("Airflow-Bronze-Transactions")
            spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
            try:
                ingest_to_bronze(spark, "s3a://source/banking/transactions.csv", "s3a://bronze/transactions")
            finally:
                spark.stop()
        
        @task(task_id='ingest_aml_alerts', execution_timeout=timedelta(minutes=30))
        def ingest_aml_alerts():
            """Ingest AML alert data from source CSV to Bronze Delta Lake"""
            sys.path.insert(0, '/opt/spark-jobs/bronze')
            from ingest_aml_alerts import create_spark_session, ingest_to_bronze
            
            spark = create_spark_session("Airflow-Bronze-AML-Alerts")
            spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
            try:
                ingest_to_bronze(spark, "s3a://source/banking/aml_alerts.csv", "s3a://bronze/aml_alerts")
            finally:
                spark.stop()
        
        # All Bronze tasks run in parallel
        [ingest_customers(), ingest_accounts(), ingest_transactions(), ingest_aml_alerts()]
    
    # ========================================================================
    # SILVER & GOLD (dbt via BashOperator)
    # ========================================================================
    
    @task_group(group_id='silver_transformation')
    def silver_transformation_group():
        """
        dbt Silver layer transformations with testing
        
        Interview point: "dbt handles the SQL transformations while Airflow
        handles orchestration — each tool does what it's best at"
        """
        dbt_run_silver = BashOperator(
            task_id='dbt_run_silver',
            bash_command='cd /opt/airflow/dbt_banking_lakehouse && dbt run --select silver.* || dbt run --full-refresh --select silver.*',
            execution_timeout=timedelta(hours=1),
        )
        
        dbt_test_silver = BashOperator(
            task_id='dbt_test_silver',
            bash_command='cd /opt/airflow/dbt_banking_lakehouse && dbt test --select silver.*',
            execution_timeout=timedelta(minutes=30),
        )
        
        dbt_run_silver >> dbt_test_silver
    
    @task_group(group_id='gold_aggregations')
    def gold_aggregations_group():
        """
        dbt Gold layer aggregations — parallel execution
        
        Interview point: "Gold models are independent, so we parallelize
        for maximum throughput"
        """
        # Run all Gold models in a single dbt invocation to avoid
        # Derby metastore lock conflicts from parallel Spark sessions
        dbt_run_gold = BashOperator(
            task_id='dbt_run_gold',
            bash_command=(
                'cd /opt/airflow/dbt_banking_lakehouse && '
                'dbt run --select customer_360 aml_compliance_dashboard '
                'cbuae_monthly_report ml_features_churn_prediction transaction_analytics '
                '|| dbt run --full-refresh --select customer_360 aml_compliance_dashboard '
                'cbuae_monthly_report ml_features_churn_prediction transaction_analytics'
            ),
            execution_timeout=timedelta(hours=1),
        )
    
    # ========================================================================
    # MARKERS & POST-PROCESSING
    # ========================================================================
    
    @task(task_id='proceed_to_silver')
    def proceed_to_silver_marker() -> str:
        logging.info("✅ Quality gate passed - proceeding to Silver layer")
        return "QUALITY_GATE_PASSED"
    
    @task(task_id='quality_failed')
    def quality_failed_marker() -> str:
        logging.error("❌ Quality gate failed - pipeline halted")
        raise ValueError("Data quality validation failed - see logs for details")
    
    generate_docs = BashOperator(
        task_id='generate_dbt_docs',
        bash_command='cd /opt/airflow/dbt_banking_lakehouse && dbt docs generate',
        execution_timeout=timedelta(minutes=10),
    )
    
    # ========================================================================
    # PIPELINE FLOW
    # ========================================================================
    
    # Stage 0: Pre-flight checks
    source_files = check_source_data_available()
    
    # Stage 1: Bronze ingestion (parallel PySpark tasks)
    bronze_tasks = bronze_ingestion_group()
    
    # Stage 2: Validation
    validation_results = validate_bronze_counts()
    
    # Stage 3: Quality gate (branching)
    quality_decision = evaluate_data_quality(validation_results)
    
    # Branch paths
    proceed_marker = proceed_to_silver_marker()
    failed_marker = quality_failed_marker()
    
    # Stage 4: Silver transformation
    silver_tasks = silver_transformation_group()
    
    # Stage 5: Gold aggregations
    gold_tasks = gold_aggregations_group()
    
    # Stage 6: Post-processing
    metrics = publish_pipeline_metrics(validation_results)
    
    # ========================================================================
    # DEPENDENCIES
    # ========================================================================
    
    source_files >> bronze_tasks >> validation_results >> quality_decision
    quality_decision >> [proceed_marker, failed_marker]
    proceed_marker >> silver_tasks >> gold_tasks >> generate_docs >> metrics


# Instantiate the DAG
banking_lakehouse_dag = banking_lakehouse_pipeline()