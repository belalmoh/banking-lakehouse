"""
Banking Lakehouse - End-to-End Pipeline (TaskFlow API)
============================================================
Orchestrates Bronze → Silver → Gold transformation with data quality gates

Interview talking points:
- "Using TaskFlow API for cleaner, more maintainable code"
- "Automatic XCom passing between tasks with type hints"
- "Better error handling with Python decorators"
- "Production-ready with SLA monitoring and quality gates"
"""

from airflow.decorators import dag, task, task_group
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

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

SPARK_CONF = {
    'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
    'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
    'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
    'spark.hadoop.fs.s3a.access.key': 'admin',
    'spark.hadoop.fs.s3a.secret.key': 'admin123',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
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
    
    Interview point: "TaskFlow API provides cleaner syntax, automatic XCom 
    serialization, and better IDE support with type hints"
    """
    
    # ========================================================================
    # TASK DEFINITIONS (Using Decorators)
    # ========================================================================
    
    @task(
        task_id='check_source_data',
        doc_md="""
        Validates that all required source files exist in S3 source bucket.
        Returns list of available files or raises error if any missing.
        """
    )
    def check_source_data_available() -> List[str]:
        """
        Check if source data is available before starting pipeline
        
        Returns:
            List of available source files
        
        Raises:
            FileNotFoundError: If any required files are missing
        """
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
            except Exception as e:
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
        doc_md="""
        Validates Bronze layer record counts match expected values.
        Acts as data quality gate before Silver processing.
        """
    )
    def validate_bronze_counts() -> Dict[str, Any]:
        """
        Validate Bronze layer record counts
        
        Returns:
            Dictionary with validation results for each table
        
        Raises:
            ValueError: If any table has insufficient records
        """
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        builder = SparkSession.builder \
            .appName("Validate-Bronze-Counts") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Expected minimum counts
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
            
            log_msg = f"{table_name}: Expected≥{expected_min:,}, Actual={actual_count:,}"
            if is_valid:
                logging.info(f"✅ {log_msg}")
            else:
                logging.error(f"❌ {log_msg}")
                all_valid = False
        
        spark.stop()
        
        if not all_valid:
            raise ValueError(
                f"Bronze layer validation failed. Results: {validation_results}"
            )
        
        logging.info("✅ All Bronze tables validated successfully")
        return validation_results
    
    @task.branch(
        task_id='data_quality_gate',
        doc_md="""
        Branch operator that routes to Silver processing if quality checks pass,
        or to failure handler if quality issues detected.
        """
    )
    def evaluate_data_quality(validation_results: Dict[str, Any]) -> str:
        """
        Evaluate data quality and branch accordingly
        
        Args:
            validation_results: Results from Bronze validation
        
        Returns:
            Task ID to execute next ('proceed_to_silver' or 'quality_failed')
        """
        all_valid = all(
            result['valid'] 
            for result in validation_results.values()
        )
        
        if all_valid:
            logging.info("✅ Data quality checks passed - proceeding to Silver")
            return 'proceed_to_silver'
        else:
            logging.error("❌ Data quality checks failed - halting pipeline")
            return 'quality_failed'
    
    @task(
        task_id='publish_metrics',
        doc_md="""
        Publishes pipeline execution metrics for monitoring dashboard.
        Tracks duration, record counts, quality scores.
        """
    )
    def publish_pipeline_metrics(validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Publish pipeline execution metrics
        
        Args:
            validation_results: Validation results to include in metrics
        
        Returns:
            Dictionary of metrics published
        """
        from airflow.models import TaskInstance
        from airflow.utils.session import provide_session
        
        metrics = {
            'pipeline_name': 'banking_lakehouse',
            'execution_timestamp': datetime.now().isoformat(),
            'status': 'SUCCESS',
            'bronze_validation': validation_results,
            'total_records_processed': sum(
                result['actual_count'] 
                for result in validation_results.values()
            ),
        }
        
        logging.info(f"📊 Pipeline Metrics: {metrics}")
        
        # In production, send to CloudWatch/Datadog
        # cloudwatch.put_metric_data(...)
        
        return metrics
    
    # ========================================================================
    # TASK GROUPS (Bronze Ingestion)
    # ========================================================================
    
    @task_group(group_id='bronze_ingestion')
    def bronze_ingestion_group():
        """
        Parallel Bronze layer ingestion tasks
        
        Interview point: "TaskFlow API task groups provide better organization
        and visual hierarchy in the Airflow UI"
        """
        
        ingest_customers = SparkSubmitOperator(
            task_id='ingest_customers',
            application='/opt/spark-jobs/bronze/ingest_customers.py',
            conn_id='spark_default',
            conf=SPARK_CONF,
            packages='io.delta:delta-core_2.12:2.4.0',
            executor_memory='2g',
            driver_memory='2g',
            name='bronze_customers',
            execution_timeout=timedelta(minutes=30),
        )
        
        ingest_accounts = SparkSubmitOperator(
            task_id='ingest_accounts',
            application='/opt/spark-jobs/bronze/ingest_accounts.py',
            conn_id='spark_default',
            conf=SPARK_CONF,
            packages='io.delta:delta-core_2.12:2.4.0',
            executor_memory='2g',
            driver_memory='2g',
            name='bronze_accounts',
            execution_timeout=timedelta(minutes=30),
        )
        
        ingest_transactions = SparkSubmitOperator(
            task_id='ingest_transactions',
            application='/opt/spark-jobs/bronze/ingest_transactions.py',
            conn_id='spark_default',
            conf=SPARK_CONF,
            packages='io.delta:delta-core_2.12:2.4.0',
            executor_memory='4g',
            driver_memory='2g',
            name='bronze_transactions',
            execution_timeout=timedelta(hours=1),
        )
        
        ingest_aml_alerts = SparkSubmitOperator(
            task_id='ingest_aml_alerts',
            application='/opt/spark-jobs/bronze/ingest_aml_alerts.py',
            conn_id='spark_default',
            conf=SPARK_CONF,
            packages='io.delta:delta-core_2.12:2.4.0',
            executor_memory='2g',
            driver_memory='2g',
            name='bronze_aml_alerts',
            execution_timeout=timedelta(minutes=30),
        )
        
        # All Bronze tasks run in parallel
        [ingest_customers, ingest_accounts, ingest_transactions, ingest_aml_alerts]
    
    @task_group(group_id='silver_transformation')
    def silver_transformation_group():
        """
        dbt Silver layer transformations with testing
        
        Interview point: "Separating dbt run and test into distinct tasks
        provides better granularity for monitoring and debugging"
        """
        
        dbt_run_silver = BashOperator(
            task_id='dbt_run_silver',
            bash_command='cd /opt/airflow/dbt_banking_lakehouse && dbt run --select silver.*',
            execution_timeout=timedelta(hours=1),
        )
        
        dbt_test_silver = BashOperator(
            task_id='dbt_test_silver',
            bash_command='cd /opt/airflow/dbt_banking_lakehouse && dbt test --select silver.*',
            execution_timeout=timedelta(minutes=30),
        )
        
        # Sequential: run then test
        dbt_run_silver >> dbt_test_silver
    
    @task_group(group_id='gold_aggregations')
    def gold_aggregations_group():
        """
        dbt Gold layer aggregations - parallel execution
        
        Interview point: "Gold models are independent, so we parallelize
        for maximum throughput. Customer 360 and AML dashboard can build
        simultaneously since they don't depend on each other"
        """
        
        dbt_customer_360 = BashOperator(
            task_id='dbt_customer_360',
            bash_command='cd /opt/airflow/dbt_banking_lakehouse && dbt run --select customer_360',
            execution_timeout=timedelta(minutes=45),
        )
        
        dbt_aml_dashboard = BashOperator(
            task_id='dbt_aml_dashboard',
            bash_command='cd /opt/airflow/dbt_banking_lakehouse && dbt run --select aml_compliance_dashboard',
            execution_timeout=timedelta(minutes=30),
        )
        
        dbt_regulatory_report = BashOperator(
            task_id='dbt_regulatory_report',
            bash_command='cd /opt/airflow/dbt_banking_lakehouse && dbt run --select cbuae_monthly_report',
            execution_timeout=timedelta(minutes=30),
        )
        
        dbt_ml_features = BashOperator(
            task_id='dbt_ml_features',
            bash_command='cd /opt/airflow/dbt_banking_lakehouse && dbt run --select ml_features_churn_prediction',
            execution_timeout=timedelta(minutes=30),
        )
        
        # All Gold models run in parallel
        [dbt_customer_360, dbt_aml_dashboard, dbt_regulatory_report, dbt_ml_features]
    
    # ========================================================================
    # ADDITIONAL TASKS
    # ========================================================================
    
    @task(task_id='proceed_to_silver')
    def proceed_to_silver_marker() -> str:
        """Marker task indicating quality gate passed"""
        logging.info("✅ Quality gate passed - proceeding to Silver layer")
        return "QUALITY_GATE_PASSED"
    
    @task(task_id='quality_failed')
    def quality_failed_marker() -> str:
        """Marker task indicating quality gate failed"""
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
    
    # Stage 1: Bronze ingestion (parallel)
    bronze_tasks = bronze_ingestion_group()
    
    # Stage 2: Validation
    validation_results = validate_bronze_counts()
    
    # Stage 3: Quality gate (branching)
    quality_decision = evaluate_data_quality(validation_results)
    
    # Branch paths
    proceed_marker = proceed_to_silver_marker()
    failed_marker = quality_failed_marker()
    
    # Stage 4: Silver transformation (if quality passed)
    silver_tasks = silver_transformation_group()
    
    # Stage 5: Gold aggregations
    gold_tasks = gold_aggregations_group()
    
    # Stage 6: Post-processing
    metrics = publish_pipeline_metrics(validation_results)
    
    # ========================================================================
    # DEPENDENCIES
    # ========================================================================
    
    # Linear flow with branching
    source_files >> bronze_tasks >> validation_results >> quality_decision
    
    # Quality gate branches
    quality_decision >> [proceed_marker, failed_marker]
    
    # Success path
    proceed_marker >> silver_tasks >> gold_tasks >> generate_docs >> metrics
    
    # Failure path ends at marker
    # (failed_marker has no downstream tasks)


# Instantiate the DAG
banking_lakehouse_dag = banking_lakehouse_pipeline()