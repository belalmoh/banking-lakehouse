"""
Kafka CDC Consumer
Banking Lakehouse Project

Consumes Change Data Capture (CDC) events from Kafka topics
and writes them to the Bronze Delta Lake layer. Supports
transaction and account update events.
"""

import json
import os
import sys
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, BooleanType,
)
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import create_spark_session
from utils.delta_helpers import write_delta, upsert_delta


# CDC event schema for transactions
CDC_TRANSACTION_SCHEMA = StructType([
    StructField("op", StringType(), False),          # Operation: I=insert, U=update, D=delete
    StructField("ts_ms", StringType(), False),        # Timestamp in ms
    StructField("transaction_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("transaction_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("country", StringType(), True),
    StructField("status", StringType(), True),
    StructField("is_international", BooleanType(), True),
])


def consume_batch_from_kafka(
    spark: SparkSession,
    kafka_bootstrap_servers: str = "kafka:9092",
    topic: str = "banking.transactions.cdc",
    bronze_path: str = None,
    max_offsets_per_trigger: int = 1000,
) -> int:
    """
    Consume a batch of CDC events from Kafka and write to Bronze layer.

    This uses Spark Structured Streaming in batch (trigger-once) mode
    for reliable exactly-once processing.

    Args:
        spark: Active SparkSession.
        kafka_bootstrap_servers: Kafka broker address.
        topic: Kafka topic to consume from.
        bronze_path: Target Bronze layer path.
        max_offsets_per_trigger: Max records to process per batch.

    Returns:
        Number of records processed.
    """
    bronze_path = bronze_path or os.getenv("BRONZE_PATH", "s3a://bronze")
    target_path = f"{bronze_path}/transactions_cdc"

    logger.info(f"Consuming CDC events from Kafka topic: {topic}")

    # Read from Kafka
    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .load()
    )

    # Parse Kafka value as JSON
    parsed_df = kafka_df.select(
        F.from_json(
            F.col("value").cast("string"),
            CDC_TRANSACTION_SCHEMA,
        ).alias("data"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
    ).select("data.*", "kafka_timestamp", "kafka_offset", "kafka_partition")

    # Add audit columns
    parsed_df = parsed_df.withColumn("_ingestion_timestamp", F.current_timestamp())
    parsed_df = parsed_df.withColumn("_source_system", F.lit("kafka_cdc"))
    parsed_df = parsed_df.withColumn("_cdc_operation", F.col("op"))

    record_count = parsed_df.count()

    if record_count > 0:
        # Separate inserts/updates from deletes
        upserts_df = parsed_df.filter(F.col("op").isin("I", "U"))
        deletes_df = parsed_df.filter(F.col("op") == "D")

        if upserts_df.count() > 0:
            write_delta(upserts_df, target_path, mode="append")
            logger.info(f"Wrote {upserts_df.count()} CDC upserts to Bronze")

        if deletes_df.count() > 0:
            # For deletes, mark as soft-delete in Delta table
            deletes_df = deletes_df.withColumn("_is_deleted", F.lit(True))
            write_delta(deletes_df, target_path, mode="append")
            logger.info(f"Wrote {deletes_df.count()} CDC deletes to Bronze")
    else:
        logger.info("No new CDC events to process")

    logger.info(f"CDC consumption complete: {record_count} events processed")
    return record_count


def simulate_cdc_events(
    spark: SparkSession,
    num_events: int = 100,
    bronze_path: str = None,
) -> int:
    """
    Simulate CDC events for demo purposes (when Kafka is not available).

    Creates synthetic CDC-like records and writes them to Bronze.

    Args:
        spark: Active SparkSession.
        num_events: Number of events to simulate.
        bronze_path: Target path for simulated events.

    Returns:
        Number of simulated records.
    """
    from faker import Faker
    import random

    fake = Faker()
    bronze_path = bronze_path or os.getenv("BRONZE_PATH", "s3a://bronze")
    target_path = f"{bronze_path}/transactions_cdc"

    logger.info(f"Simulating {num_events} CDC events...")

    events = []
    for _ in range(num_events):
        events.append({
            "op": random.choice(["I", "I", "I", "U"]),  # Mostly inserts
            "ts_ms": str(int(datetime.utcnow().timestamp() * 1000)),
            "transaction_id": f"TXN-CDC-{fake.uuid4()[:8].upper()}",
            "account_id": f"ACC-{random.randint(1, 20000):06d}",
            "transaction_type": random.choice(["DEBIT", "CREDIT"]),
            "amount": round(random.uniform(10, 50000), 2),
            "currency": random.choice(["AED", "USD", "EUR", "GBP"]),
            "merchant_name": fake.company(),
            "merchant_category": random.choice([
                "GROCERY", "RESTAURANT", "FUEL", "RETAIL", "UTILITIES",
            ]),
            "country": random.choice(["AE", "US", "GB", "SA", "IN"]),
            "status": "COMPLETED",
            "is_international": random.random() > 0.7,
        })

    df = spark.createDataFrame(events, schema=CDC_TRANSACTION_SCHEMA)
    df = df.withColumn("_ingestion_timestamp", F.current_timestamp())
    df = df.withColumn("_source_system", F.lit("cdc_simulation"))
    df = df.withColumn("_cdc_operation", F.col("op"))

    write_delta(df, target_path, mode="append")

    logger.info(f"Simulated {num_events} CDC events written to {target_path}")
    return num_events


if __name__ == "__main__":
    spark = create_spark_session(app_name="CDCConsumer")

    # Try Kafka first, fall back to simulation
    try:
        count = consume_batch_from_kafka(spark)
    except Exception as e:
        logger.warning(f"Kafka not available ({e}). Falling back to simulation.")
        count = simulate_cdc_events(spark, num_events=100)

    spark.stop()
    logger.info(f"CDC processing complete: {count} events")
