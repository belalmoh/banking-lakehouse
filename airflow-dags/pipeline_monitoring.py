"""
Pipeline Monitoring & Alerting (TaskFlow API)
==============================================
Monitors lakehouse health and sends alerts for anomalies
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='pipeline_monitoring',
    default_args=DEFAULT_ARGS,
    description='Monitor lakehouse pipeline health and data quality',
    schedule='0 */4 * * *',  # Every 4 hours
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['monitoring', 'observability', 'taskflow'],
)
def pipeline_monitoring():
    """
    Monitoring DAG using TaskFlow API
    
    Interview point: "TaskFlow API makes monitoring code more readable
    with automatic dependency inference from function calls"
    """
    
    @task(
        task_id='check_sla_breaches',
        doc_md="""
        Monitors recent pipeline runs for SLA breaches.
        Critical for regulatory compliance tracking.
        """
    )
    def check_pipeline_sla_breaches() -> Dict[str, Any]:
        """
        Check for SLA breaches in recent pipeline runs
        
        Returns:
            Dictionary with SLA breach information
        """
        from airflow.models import DagRun
        from airflow.utils.db import provide_session
        
        @provide_session
        def get_recent_runs(session=None):
            runs = session.query(DagRun).filter(
                DagRun.dag_id == 'banking_lakehouse_pipeline'
            ).order_by(DagRun.execution_date.desc()).limit(10).all()
            return runs
        
        runs = get_recent_runs()
        sla_breaches = []
        sla_threshold_hours = 4
        
        for run in runs:
            if run.end_date and run.start_date:
                duration_hours = (run.end_date - run.start_date).total_seconds() / 3600
                
                breach_info = {
                    'execution_date': str(run.execution_date),
                    'duration_hours': round(duration_hours, 2),
                    'state': run.state,
                    'breached': duration_hours > sla_threshold_hours
                }
                
                if breach_info['breached']:
                    sla_breaches.append(breach_info)
                    logging.warning(
                        f"⚠️ SLA BREACH: {run.execution_date} took {duration_hours:.2f}h"
                    )
        
        result = {
            'total_runs_checked': len(runs),
            'sla_breaches': sla_breaches,
            'breach_count': len(sla_breaches),
            'breach_rate': len(sla_breaches) / len(runs) if runs else 0,
        }
        
        if sla_breaches:
            logging.error(f"❌ Found {len(sla_breaches)} SLA breaches!")
        else:
            logging.info("✅ No SLA breaches detected")
        
        return result
    
    @task(
        task_id='check_data_freshness',
        doc_md="""
        Verifies Gold layer data freshness.
        Ensures business dashboards have current data.
        """
    )
    def check_data_freshness() -> Dict[str, Any]:
        """
        Check data freshness in Gold layer
        
        Returns:
            Dictionary with freshness metrics
        """
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        builder = SparkSession.builder \
            .appName("Check-Data-Freshness") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Check Customer 360 freshness
        customer_360 = spark.read.format("delta").load("s3a://gold/customer_360")
        latest_update = customer_360.agg({"gold_updated_at": "max"}).collect()[0][0]
        
        hours_since_update = (datetime.now() - latest_update).total_seconds() / 3600
        freshness_threshold_hours = 26  # Daily + 2 hour buffer
        
        result = {
            'table': 'customer_360',
            'last_updated': str(latest_update),
            'hours_since_update': round(hours_since_update, 2),
            'threshold_hours': freshness_threshold_hours,
            'is_fresh': hours_since_update <= freshness_threshold_hours,
        }
        
        if result['is_fresh']:
            logging.info(
                f"✅ Data fresh: Last updated {hours_since_update:.2f}h ago"
            )
        else:
            logging.error(
                f"❌ STALE DATA: Last updated {hours_since_update:.2f}h ago "
                f"(threshold: {freshness_threshold_hours}h)"
            )
        
        spark.stop()
        return result
    
    @task(
        task_id='check_row_counts',
        doc_md="""
        Monitors table row counts for sudden drops.
        Early indicator of data loss or pipeline issues.
        """
    )
    def check_table_row_counts() -> Dict[str, Any]:
        """
        Check for unexpected drops in row counts
        
        Returns:
            Dictionary with row count validations
        """
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip
        
        builder = SparkSession.builder \
            .appName("Check-Row-Counts") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "admin") \
            .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        tables_to_check = {
            'bronze/customers': 10000,
            'bronze/accounts': 20000,
            'bronze/transactions': 100000,
            'bronze/aml_alerts': 5000,
            'silver/customers_silver': 10000,
            'silver/accounts_silver': 20000,
            'gold/customer_360': 10000,
        }
        
        validations = {}
        issues = []
        
        for table_path, expected_min in tables_to_check.items():
            actual_count = spark.read.format("delta").load(f"s3a://{table_path}").count()
            
            is_valid = actual_count >= expected_min
            variance_pct = round((actual_count - expected_min) / expected_min * 100, 2)
            
            validations[table_path] = {
                'expected_min': expected_min,
                'actual_count': actual_count,
                'is_valid': is_valid,
                'variance_pct': variance_pct,
            }
            
            if is_valid:
                logging.info(f"✅ {table_path}: {actual_count:,} rows (variance: {variance_pct}%)")
            else:
                logging.error(f"❌ {table_path}: {actual_count:,} < {expected_min:,}")
                issues.append(table_path)
        
        spark.stop()
        
        result = {
            'tables_checked': len(tables_to_check),
            'validations': validations,
            'issue_count': len(issues),
            'issues': issues,
        }
        
        if issues:
            logging.error(f"❌ Found {len(issues)} table count issues!")
        else:
            logging.info("✅ All table counts within expected range")
        
        return result
    
    @task(
        task_id='aggregate_monitoring_results',
        doc_md="""
        Aggregates all monitoring check results.
        Sends consolidated alert if any issues detected.
        """
    )
    def aggregate_monitoring_results(
        sla_results: Dict[str, Any],
        freshness_results: Dict[str, Any],
        count_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate all monitoring results and trigger alerts if needed
        
        Args:
            sla_results: SLA breach check results
            freshness_results: Data freshness check results
            count_results: Row count check results
        
        Returns:
            Consolidated monitoring report
        """
        
        # Determine overall health status
        has_issues = (
            sla_results['breach_count'] > 0 or
            not freshness_results['is_fresh'] or
            count_results['issue_count'] > 0
        )
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'UNHEALTHY' if has_issues else 'HEALTHY',
            'sla_status': sla_results,
            'freshness_status': freshness_results,
            'row_count_status': count_results,
        }
        
        # Log summary
        logging.info("=" * 80)
        logging.info("MONITORING SUMMARY")
        logging.info("=" * 80)
        logging.info(f"Overall Status: {report['overall_status']}")
        logging.info(f"SLA Breaches: {sla_results['breach_count']}")
        logging.info(f"Data Freshness: {'✅ Fresh' if freshness_results['is_fresh'] else '❌ Stale'}")
        logging.info(f"Row Count Issues: {count_results['issue_count']}")
        logging.info("=" * 80)
        
        if has_issues:
            # In production: send to PagerDuty, Slack, SNS
            logging.warning("⚠️ Issues detected - alerts should be sent to on-call team")
            # slack_alert(report)
            # pagerduty_alert(report)
        
        return report
    
    # ========================================================================
    # PIPELINE FLOW (Automatic dependency inference)
    # ========================================================================
    
    sla_check = check_pipeline_sla_breaches()
    freshness_check = check_data_freshness()
    count_check = check_table_row_counts()
    
    # TaskFlow API automatically infers dependencies from function arguments
    final_report = aggregate_monitoring_results(
        sla_check,
        freshness_check, 
        count_check
    )


# Instantiate the DAG
monitoring_dag = pipeline_monitoring()