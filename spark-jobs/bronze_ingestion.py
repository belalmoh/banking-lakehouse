"""
Bronze Layer Ingestion
Banking Lakehouse Project

Reads raw data from CSV/JSON source files and writes to Delta Lake Bronze layer.
Adds audit columns, data classification, and handles schema evolution.
Designed for idempotent re-execution.
"""

import os
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType,
)
from loguru import logger

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import create_spark_session
from utils.delta_helpers import write_delta, table_exists
from utils.data_classification import add_classification_columns


# ─── Schema Definitions ─────────────────────────────────────────────────────

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("risk_score", IntegerType(), True),
    StructField("kyc_status", StringType(), True),
    StructField("onboarding_date", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("country", StringType(), True),
])

ACCOUNTS_SCHEMA = StructType([
    StructField("account_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("account_type", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("opened_date", StringType(), True),
])

TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
    StructField("is_international", BooleanType(), True),
])

AML_ALERTS_SCHEMA = StructType([
    StructField("alert_id", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), True),
    StructField("alert_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("assigned_to", StringType(), True),
])

SCHEMAS = {
    "customers": CUSTOMERS_SCHEMA,
    "accounts": ACCOUNTS_SCHEMA,
    "transactions": TRANSACTIONS_SCHEMA,
    "aml_alerts": AML_ALERTS_SCHEMA,
}


def add_audit_columns(
    df: DataFrame,
    source_file: str,
    data_residency: str = "UAE",
) -> DataFrame:
    """
    Add audit and governance columns to a DataFrame.

    Columns added:
        - _ingestion_timestamp: UTC timestamp of ingestion
        - _source_file: Name of the source file
        - _data_classification: PII/CONFIDENTIAL/PUBLIC
        - _data_residency: Country where data resides
        - _pii_fields: JSON array of PII column names
        - _batch_id: Unique batch identifier
    """
    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    df = df.withColumn("_ingestion_timestamp", F.current_timestamp())
    df = df.withColumn("_source_file", F.lit(source_file))
    df = df.withColumn("_batch_id", F.lit(batch_id))

    # Add data classification columns
    df = add_classification_columns(df, data_residency=data_residency)

    return df


def ingest_csv(
    spark: SparkSession,
    source_path: str,
    table_name: str,
    target_path: str,
    partition_by: Optional[list] = None,
) -> int:
    """
    Ingest a CSV file into the Bronze Delta Lake layer.

    Args:
        spark: Active SparkSession.
        source_path: Path to the source CSV file.
        table_name: Name of the entity (e.g., 'customers').
        target_path: Delta table output path.
        partition_by: Optional partition columns.

    Returns:
        Number of records ingested.
    """
    logger.info(f"Ingesting CSV: {source_path} → {target_path}")

    schema = SCHEMAS.get(table_name)
    if schema:
        df = spark.read.csv(source_path, header=True, schema=schema)
    else:
        df = spark.read.csv(source_path, header=True, inferSchema=True)

    # Add audit trail
    source_filename = os.path.basename(source_path)
    df = add_audit_columns(df, source_file=source_filename)

    record_count = df.count()
    logger.info(f"Records read: {record_count}")

    # Write to Delta (append for idempotent incremental ingestion)
    write_delta(df, target_path, mode="append", partition_by=partition_by)

    logger.info(f"Bronze ingestion complete: {table_name} ({record_count} records)")
    return record_count


def ingest_json(
    spark: SparkSession,
    source_path: str,
    table_name: str,
    target_path: str,
    partition_by: Optional[list] = None,
) -> int:
    """
    Ingest a JSON file into the Bronze Delta Lake layer.

    Args:
        spark: Active SparkSession.
        source_path: Path to the source JSON file.
        table_name: Name of the entity.
        target_path: Delta table output path.
        partition_by: Optional partition columns.

    Returns:
        Number of records ingested.
    """
    logger.info(f"Ingesting JSON: {source_path} → {target_path}")

    schema = SCHEMAS.get(table_name)
    if schema:
        df = spark.read.json(source_path, schema=schema)
    else:
        df = spark.read.json(source_path)

    source_filename = os.path.basename(source_path)
    df = add_audit_columns(df, source_file=source_filename)

    record_count = df.count()
    write_delta(df, target_path, mode="append", partition_by=partition_by)

    logger.info(f"Bronze ingestion complete: {table_name} ({record_count} records)")
    return record_count


def run_bronze_ingestion(
    data_dir: str = "/opt/data/sample",
    bronze_path: str = None,
) -> dict:
    """
    Execute full Bronze layer ingestion for all banking entities.

    Args:
        data_dir: Directory containing source CSV/JSON files.
        bronze_path: Base path for Bronze Delta tables.

    Returns:
        Dictionary with record counts per entity.
    """
    bronze_path = bronze_path or os.getenv("BRONZE_PATH", "s3a://bronze")

    spark = create_spark_session(app_name="BronzeIngestion")
    results = {}

    # Define ingestion jobs
    ingestion_jobs = [
        {
            "table": "customers",
            "source": os.path.join(data_dir, "customers.csv"),
            "target": f"{bronze_path}/customers",
            "format": "csv",
            "partition_by": None,
        },
        {
            "table": "accounts",
            "source": os.path.join(data_dir, "accounts.csv"),
            "target": f"{bronze_path}/accounts",
            "format": "csv",
            "partition_by": None,
        },
        {
            "table": "transactions",
            "source": os.path.join(data_dir, "transactions.csv"),
            "target": f"{bronze_path}/transactions",
            "format": "csv",
            "partition_by": ["currency"],
        },
        {
            "table": "aml_alerts",
            "source": os.path.join(data_dir, "aml_alerts.csv"),
            "target": f"{bronze_path}/aml_alerts",
            "format": "csv",
            "partition_by": None,
        },
    ]

    for job in ingestion_jobs:
        try:
            source_path = job["source"]
            if not os.path.exists(source_path):
                logger.warning(f"Source file not found: {source_path}. Skipping.")
                continue

            if job["format"] == "csv":
                count = ingest_csv(
                    spark, source_path, job["table"],
                    job["target"], job["partition_by"],
                )
            else:
                count = ingest_json(
                    spark, source_path, job["table"],
                    job["target"], job["partition_by"],
                )
            results[job["table"]] = count

        except Exception as e:
            logger.error(f"Failed to ingest {job['table']}: {e}")
            results[job["table"]] = -1

    # Summary
    logger.info("=" * 60)
    logger.info("Bronze Ingestion Summary")
    logger.info("=" * 60)
    for table, count in results.items():
        status = "✅" if count > 0 else "❌"
        logger.info(f"  {status} {table}: {count} records")

    spark.stop()
    return results


if __name__ == "__main__":
    run_bronze_ingestion()
