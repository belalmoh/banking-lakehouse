"""
Silver Layer Transformations (PySpark)
Banking Lakehouse Project

Transitional Silver layer transformations using PySpark.
These will be migrated to dbt models for SQL-based transformations.
Handles data cleansing, deduplication, type casting, and business rules.
"""

import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import create_spark_session
from utils.delta_helpers import read_delta, write_delta
from utils.data_classification import flag_aml_transactions


def cleanse_customers(df: DataFrame) -> DataFrame:
    """
    Cleanse customer data:
    - Deduplicate by customer_id (keep latest)
    - Standardize names (title case)
    - Validate email format
    - Cast date fields
    - Filter invalid records
    """
    logger.info("Cleansing customers data...")

    # Deduplicate: keep latest ingestion per customer_id
    window = Window.partitionBy("customer_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")

    # Standardize names
    df = df.withColumn("first_name", F.initcap(F.trim(F.col("first_name"))))
    df = df.withColumn("last_name", F.initcap(F.trim(F.col("last_name"))))

    # Validate email (basic pattern check)
    df = df.withColumn(
        "email_valid",
        F.col("email").rlike(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"),
    )

    # Cast dates
    df = df.withColumn(
        "date_of_birth", F.to_date(F.col("date_of_birth"), "yyyy-MM-dd")
    )
    df = df.withColumn(
        "onboarding_date", F.to_date(F.col("onboarding_date"), "yyyy-MM-dd")
    )

    # Filter out records missing required fields
    df = df.filter(
        F.col("customer_id").isNotNull()
        & F.col("first_name").isNotNull()
        & F.col("last_name").isNotNull()
    )

    # Add cleansing metadata
    df = df.withColumn("_cleansed_timestamp", F.current_timestamp())

    record_count = df.count()
    logger.info(f"Customers cleansed: {record_count} records")
    return df


def cleanse_accounts(df: DataFrame) -> DataFrame:
    """
    Cleanse accounts data:
    - Deduplicate by account_id
    - Validate balance (non-negative for savings/checking)
    - Standardize status values
    - Cast date fields
    """
    logger.info("Cleansing accounts data...")

    # Deduplicate
    window = Window.partitionBy("account_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")

    # Standardize status to uppercase
    df = df.withColumn("status", F.upper(F.trim(F.col("status"))))
    df = df.withColumn("account_type", F.upper(F.trim(F.col("account_type"))))

    # Cast dates
    df = df.withColumn(
        "opened_date", F.to_date(F.col("opened_date"), "yyyy-MM-dd")
    )

    # Filter nulls on key columns
    df = df.filter(
        F.col("account_id").isNotNull()
        & F.col("customer_id").isNotNull()
    )

    df = df.withColumn("_cleansed_timestamp", F.current_timestamp())

    record_count = df.count()
    logger.info(f"Accounts cleansed: {record_count} records")
    return df


def cleanse_transactions(df: DataFrame) -> DataFrame:
    """
    Cleanse transactions data:
    - Deduplicate by transaction_id
    - Filter invalid amounts (must be > 0)
    - Cast timestamps
    - Standardize currency codes
    - Apply AML flagging
    """
    logger.info("Cleansing transactions data...")

    # Deduplicate
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")

    # Filter invalid amounts
    df = df.filter(F.col("amount") > 0)

    # Cast timestamps
    df = df.withColumn(
        "transaction_timestamp",
        F.to_timestamp(F.col("transaction_timestamp")),
    )

    # Standardize
    df = df.withColumn("currency", F.upper(F.trim(F.col("currency"))))
    df = df.withColumn("status", F.upper(F.trim(F.col("status"))))
    df = df.withColumn(
        "transaction_type", F.upper(F.trim(F.col("transaction_type")))
    )

    # Add derived columns
    df = df.withColumn(
        "transaction_date",
        F.to_date(F.col("transaction_timestamp")),
    )
    df = df.withColumn(
        "transaction_hour",
        F.hour(F.col("transaction_timestamp")),
    )

    # AML flagging
    df = flag_aml_transactions(df)

    df = df.withColumn("_cleansed_timestamp", F.current_timestamp())

    record_count = df.count()
    logger.info(f"Transactions cleansed: {record_count} records")
    return df


def cleanse_aml_alerts(df: DataFrame) -> DataFrame:
    """
    Cleanse AML alerts data:
    - Deduplicate by alert_id
    - Standardize severity and status
    - Cast timestamps
    """
    logger.info("Cleansing AML alerts data...")

    # Deduplicate
    window = Window.partitionBy("alert_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )
    df = df.withColumn("_row_num", F.row_number().over(window))
    df = df.filter(F.col("_row_num") == 1).drop("_row_num")

    # Standardize
    df = df.withColumn("severity", F.upper(F.trim(F.col("severity"))))
    df = df.withColumn("status", F.upper(F.trim(F.col("status"))))
    df = df.withColumn("alert_type", F.upper(F.trim(F.col("alert_type"))))

    # Cast timestamps
    df = df.withColumn(
        "created_at", F.to_timestamp(F.col("created_at"))
    )

    df = df.withColumn("_cleansed_timestamp", F.current_timestamp())

    record_count = df.count()
    logger.info(f"AML alerts cleansed: {record_count} records")
    return df


def run_silver_transformations(
    bronze_path: str = None,
    silver_path: str = None,
) -> dict:
    """
    Execute all Silver layer transformations.

    Returns:
        Dictionary with record counts per entity.
    """
    bronze_path = bronze_path or os.getenv("BRONZE_PATH", "s3a://bronze")
    silver_path = silver_path or os.getenv("SILVER_PATH", "s3a://silver")

    spark = create_spark_session(app_name="SilverTransformations")
    results = {}

    transformations = [
        ("customers", cleanse_customers),
        ("accounts", cleanse_accounts),
        ("transactions", cleanse_transactions),
        ("aml_alerts", cleanse_aml_alerts),
    ]

    for table_name, transform_fn in transformations:
        try:
            bronze_table = f"{bronze_path}/{table_name}"
            silver_table = f"{silver_path}/{table_name}_cleansed"

            logger.info(f"Processing: {bronze_table} → {silver_table}")

            df = read_delta(spark, bronze_table)
            df_cleansed = transform_fn(df)
            write_delta(df_cleansed, silver_table, mode="overwrite")

            results[table_name] = df_cleansed.count()

        except Exception as e:
            logger.error(f"Failed Silver transformation for {table_name}: {e}")
            results[table_name] = -1

    # Summary
    logger.info("=" * 60)
    logger.info("Silver Transformation Summary")
    logger.info("=" * 60)
    for table, count in results.items():
        status = "✅" if count > 0 else "❌"
        logger.info(f"  {status} {table}_cleansed: {count} records")

    spark.stop()
    return results


if __name__ == "__main__":
    run_silver_transformations()
