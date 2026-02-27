"""
Bronze Layer Ingestion - Accounts
Similar pattern to customers with account-specific validations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip
from datetime import datetime

def create_spark_session(app_name="Bronze-Accounts"):
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

def get_account_schema():
    return StructType([
        StructField("account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("account_type", StringType(), False),
        StructField("currency", StringType(), False),
        StructField("balance", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("opened_date", DateType(), False),
        StructField("last_activity_date", DateType(), True),
        StructField("data_classification", StringType(), False),
        StructField("created_at", TimestampType(), False)
    ])

def add_audit_columns(df):
    df = df.withColumn(
        "_record_hash",
        sha2(concat_ws("|", col("account_id"), col("customer_id"), col("balance")), 256)
    )
    
    df = df.withColumn("_ingestion_timestamp", current_timestamp())
    df = df.withColumn("_source_file", input_file_name())
    df = df.withColumn("_source_system", lit("CORE_BANKING"))
    df = df.withColumn("_data_classification", lit("CONFIDENTIAL"))  # Balance data
    df = df.withColumn("_data_residency", lit("UAE"))
    df = df.withColumn("_ingestion_date", to_date(current_timestamp()))
    
    return df

def validate_data_quality(df):
    print("\n" + "=" * 80)
    print("DATA QUALITY VALIDATION - ACCOUNTS")
    print("=" * 80)
    
    total_records = df.count()
    print(f"Total records: {total_records:,}")
    
    # Null checks
    null_account_ids = df.filter(col("account_id").isNull()).count()
    null_balances = df.filter(col("balance").isNull()).count()
    
    print(f"\nNull Checks:")
    print(f"  - Null account_id: {null_account_ids}")
    print(f"  - Null balance: {null_balances}")
    
    # Business validations
    invalid_currency = df.filter(~col("currency").isin(["AED", "USD", "EUR", "GBP", "SAR", "INR"])).count()
    invalid_status = df.filter(~col("status").isin(["ACTIVE", "DORMANT", "CLOSED"])).count()
    
    print(f"\nBusiness Rule Violations:")
    print(f"  - Invalid currency: {invalid_currency}")
    print(f"  - Invalid status: {invalid_status}")
    
    # Account type distribution
    print(f"\nAccount Type Distribution:")
    df.groupBy("account_type").count().orderBy("count", ascending=False).show()
    
    # Balance statistics
    print(f"\nBalance Statistics (AED accounts):")
    df.filter(col("currency") == "AED").select("balance").describe().show()
    
    print("=" * 80)
    return df

def ingest_to_bronze(spark, source_path, target_path, run_date=None):
    print("\n" + "=" * 80)
    print("BRONZE LAYER INGESTION - ACCOUNTS")
    print("=" * 80)
    
    # Read source
    schema = get_account_schema()
    df_source = spark.read.option("header", "true").schema(schema).csv(source_path)
    print(f"✅ Read {df_source.count():,} records")
    
    # Add audit columns
    df_with_audit = add_audit_columns(df_source)
    
    # Validate
    df_validated = validate_data_quality(df_with_audit)

    print("\n Checking for existing data...")
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
    
    # Write to Delta
    df_validated.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("_ingestion_date") \
        .save(target_path)
    
    print(f"\n✅ BRONZE INGESTION COMPLETE!")
    
    # Show Delta history
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, target_path)
    print("\n📋 Delta Transaction History:")
    delta_table.history(3).select("version", "timestamp", "operation").show(truncate=False)

def main():
    SOURCE_PATH = "s3a://source/banking/accounts.csv"
    TARGET_PATH = "s3a://bronze/accounts"
    
    spark = create_spark_session()
    
    try:
        ingest_to_bronze(spark, SOURCE_PATH, TARGET_PATH)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()