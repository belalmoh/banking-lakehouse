"""
Bronze Layer Ingestion - AML Alerts
Regulatory compliance data requiring strict audit trails
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

def create_spark_session(app_name="Bronze-AML-Alerts"):
    my_packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.hadoop:hadoop-client-runtime:3.3.4",
        "org.apache.hadoop:hadoop-client-api:3.3.4",
        "io.delta:delta-core_2.12:2.4.0",
        "io.delta:delta-storage:2.4.0",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def get_aml_schema():
    return StructType([
        StructField("alert_id", StringType(), False),
        StructField("transaction_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("alert_type", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("status", StringType(), False),
        StructField("assigned_to", StringType(), True),
        StructField("created_at", TimestampType(), False),
        StructField("last_updated", TimestampType(), False),
        StructField("resolution_notes", StringType(), True),
        StructField("customer_risk_score", IntegerType(), True),
        StructField("transaction_amount", DoubleType(), True),
        StructField("transaction_country", StringType(), True),
        StructField("data_classification", StringType(), False)
    ])

def add_audit_columns(df):
    df = df.withColumn("alert_date", to_date(col("created_at")))
    
    df = df.withColumn(
        "_record_hash",
        sha2(concat_ws("|", col("alert_id"), col("transaction_id"), col("status")), 256)
    )
    
    df = df.withColumn("_ingestion_timestamp", current_timestamp())
    df = df.withColumn("_source_file", input_file_name())
    df = df.withColumn("_source_system", lit("AML_MONITORING"))
    df = df.withColumn("_data_classification", lit("CONFIDENTIAL"))
    df = df.withColumn("_data_residency", lit("UAE"))
    df = df.withColumn("_ingestion_date", to_date(current_timestamp()))
    
    return df

def validate_data_quality(df):
    print("\n" + "=" * 80)
    print("DATA QUALITY VALIDATION - AML ALERTS")
    print("=" * 80)
    
    total_alerts = df.count()
    print(f"Total alerts: {total_alerts:,}")
    
    # Critical alerts requiring immediate action
    critical_alerts = df.filter(col("severity") == "CRITICAL").count()
    new_alerts = df.filter(col("status") == "NEW").count()
    
    print(f"\nAlert Priority:")
    print(f"  - Critical severity: {critical_alerts}")
    print(f"  - New (unassigned): {new_alerts}")
    print(f"  - Requiring immediate action: {df.filter((col('severity') == 'CRITICAL') & (col('status') == 'NEW')).count()}")
    
    # Alert type distribution
    print(f"\nAlert Type Distribution:")
    df.groupBy("alert_type").count().orderBy("count", ascending=False).show(truncate=False)
    
    # Status workflow
    print(f"\nAlert Status Workflow:")
    df.groupBy("status").count().orderBy("count", ascending=False).show()
    
    # Severity distribution
    print(f"\nSeverity Distribution:")
    df.groupBy("severity").count().show()
    
    print("=" * 80)
    return df

def ingest_to_bronze(spark, source_path, target_path, run_date=None):
    print("\n" + "=" * 80)
    print("BRONZE LAYER INGESTION - AML ALERTS")
    print("=" * 80)
    
    schema = get_aml_schema()
    df_source = spark.read.option("header", "true").schema(schema).csv(source_path)
    print(f"✅ Read {df_source.count():,} records")
    
    df_with_audit = add_audit_columns(df_source)
    df_validated = validate_data_quality(df_with_audit)

    print("\nChecking for existing data...")
    from delta.tables import DeltaTable
    
    if DeltaTable.isDeltaTable(spark, target_path):
        print("⚠️  Delta table exists - checking for duplicates...")
        
        # Read existing data for today's ingestion date
        df_existing = spark.read.format("delta").load(target_path)
        
        ingestion_date = run_date or datetime.now().date()
        existing_today = df_existing.filter(
            col("_ingestion_date") == ingestion_date
        ).count()
        
        if existing_today > 0:
            print(f"⚠️  Found {existing_today:,} records already ingested for {ingestion_date}")
            print("⚠️  Skipping ingestion to maintain idempotency")
            print("⚠️  To re-ingest, delete existing partition first or use different run_date")
            return
    else:
        print("✅ No existing table - first-time ingestion")
    
    df_validated.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("alert_date", "_ingestion_date") \
        .save(target_path)
    
    print(f"\n✅ BRONZE INGESTION COMPLETE!")

def main():
    SOURCE_PATH = "s3a://source/banking/aml_alerts.csv"
    TARGET_PATH = "s3a://bronze/aml_alerts"
    
    spark = create_spark_session()
    
    try:
        ingest_to_bronze(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()