"""
Bronze Layer Ingestion - Transactions
High-volume data requiring partition optimization

Interview point: "Transactions are partitioned by transaction_date for
efficient AML screening queries. This mimics ADCB's need to query recent
transactions quickly for real-time fraud detection."
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip
from datetime import datetime

def create_spark_session(app_name="Bronze-Transactions"):
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

def get_transaction_schema():
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), False),
        StructField("country", StringType(), False),
        StructField("is_international", BooleanType(), False),
        StructField("status", StringType(), False),
        StructField("description", StringType(), True),
        StructField("data_classification", StringType(), False),
        StructField("created_at", TimestampType(), False)
    ])

def add_audit_columns(df):
    # Add transaction_date for partitioning (critical for AML queries)
    df = df.withColumn("transaction_date", to_date(col("transaction_timestamp")))
    
    df = df.withColumn(
        "_record_hash",
        sha2(concat_ws("|", col("transaction_id"), col("amount"), col("transaction_timestamp")), 256)
    )
    
    df = df.withColumn("_ingestion_timestamp", current_timestamp())
    df = df.withColumn("_source_file", input_file_name())
    df = df.withColumn("_source_system", lit("TRANSACTION_ENGINE"))
    df = df.withColumn("_data_classification", lit("CONFIDENTIAL"))
    df = df.withColumn("_data_residency", lit("UAE"))
    df = df.withColumn("_ingestion_date", to_date(current_timestamp()))
    
    return df

def validate_data_quality(df):
    print("\n" + "=" * 80)
    print("DATA QUALITY VALIDATION - TRANSACTIONS")
    print("=" * 80)
    
    total_records = df.count()
    print(f"Total records: {total_records:,}")
    
    # Critical validations for transactions
    negative_amounts = df.filter(col("amount") < 0).count()
    null_amounts = df.filter(col("amount").isNull()).count()
    future_transactions = df.filter(col("transaction_timestamp") > current_timestamp()).count()
    
    print(f"\nData Quality Checks:")
    print(f"  - Negative amounts: {negative_amounts}")
    print(f"  - Null amounts: {null_amounts}")
    print(f"  - Future-dated transactions: {future_transactions}")
    
    # High-value transactions (AML threshold)
    high_value = df.filter(col("amount") > 50000).count()
    print(f"\nHigh-Value Transactions (>50K AED): {high_value:,}")
    print(f"  Percentage: {(high_value/total_records)*100:.2f}%")
    
    # International transactions
    international = df.filter(col("is_international") == True).count()
    print(f"\nInternational Transactions: {international:,}")
    print(f"  Percentage: {(international/total_records)*100:.2f}%")
    
    # Transaction status distribution
    print(f"\nStatus Distribution:")
    df.groupBy("status").count().show()
    
    # Top merchant categories
    print(f"\nTop 5 Merchant Categories:")
    df.groupBy("merchant_category").count().orderBy("count", ascending=False).show(5)
    
    print("=" * 80)
    return df

def ingest_to_bronze(spark, source_path, target_path, run_date=None):
    print("\n" + "=" * 80)
    print("BRONZE LAYER INGESTION - TRANSACTIONS")
    print("=" * 80)
    
    schema = get_transaction_schema()
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
    
    # Write with dual partitioning for query optimization
    print("\n[Writing to Delta Lake with partitioning...]")
    df_validated.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("transaction_date", "_ingestion_date") \
        .save(target_path)
    
    print(f"\n✅ BRONZE INGESTION COMPLETE!")
    
    # Optimization for query performance (may fail if existing data uses snappy compression)
    print("\n[Optimizing Delta table...]")
    try:
        from delta.tables import DeltaTable
        delta_table = DeltaTable.forPath(spark, target_path)
        
        # OPTIMIZE with Z-ordering on frequently filtered columns
        delta_table.optimize().executeZOrderBy("account_id", "amount")
        print("✅ Z-ordering applied on account_id and amount")
        
        delta_table.history(3).select("version", "timestamp", "operation").show(truncate=False)
    except Exception as e:
        print(f"⚠️  OPTIMIZE skipped (non-critical): {str(e)[:200]}")
        print("   This is expected when running from Airflow. Run OPTIMIZE on Spark master separately.")

def main():
    SOURCE_PATH = "s3a://source/banking/transactions.csv"
    TARGET_PATH = "s3a://bronze/transactions"
    
    spark = create_spark_session()
    
    try:
        ingest_to_bronze(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()