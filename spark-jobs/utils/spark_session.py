"""
Reusable Spark Session Factory
Banking Lakehouse Project

Creates configured SparkSession instances with Delta Lake and S3/MinIO support.
Designed for both local development and containerized deployment.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession
from loguru import logger


def create_spark_session(
    app_name: str = "BankingLakehouse",
    master: Optional[str] = None,
    enable_delta: bool = True,
    enable_s3: bool = True,
    extra_configs: Optional[dict] = None,
) -> SparkSession:
    """
    Create a configured SparkSession for the Banking Lakehouse.

    Args:
        app_name: Application name visible in Spark UI.
        master: Spark master URL. Defaults to SPARK_MASTER_URL env var or local[*].
        enable_delta: Enable Delta Lake extensions.
        enable_s3: Enable S3/MinIO connectivity.
        extra_configs: Additional Spark configuration key-value pairs.

    Returns:
        Configured SparkSession instance.
    """
    master_url = master or os.getenv("SPARK_MASTER_URL", "local[*]")

    logger.info(f"Creating SparkSession: app={app_name}, master={master_url}")

    builder = SparkSession.builder.appName(app_name).master(master_url)

    # Delta Lake configuration
    if enable_delta:
        builder = builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        ).config(
            "spark.databricks.delta.schema.autoMerge.enabled", "true"
        ).config(
            "spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true"
        )

    # S3 / MinIO configuration
    if enable_s3:
        s3_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
        s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
        s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "admin123")

        builder = builder.config(
            "spark.hadoop.fs.s3a.endpoint", s3_endpoint
        ).config(
            "spark.hadoop.fs.s3a.access.key", s3_access_key
        ).config(
            "spark.hadoop.fs.s3a.secret.key", s3_secret_key
        ).config(
            "spark.hadoop.fs.s3a.path.style.access", "true"
        ).config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        ).config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled", "false"
        )

    # Performance tuning for laptop
    builder = builder.config(
        "spark.sql.shuffle.partitions", "8"
    ).config(
        "spark.default.parallelism", "4"
    ).config(
        "spark.sql.adaptive.enabled", "true"
    ).config(
        "spark.driver.memory",
        os.getenv("SPARK_DRIVER_MEMORY", "2g"),
    ).config(
        "spark.executor.memory",
        os.getenv("SPARK_EXECUTOR_MEMORY", "2g"),
    )

    # Apply extra configs
    if extra_configs:
        for key, value in extra_configs.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"SparkSession created: version={spark.version}")
    return spark


def get_or_create_spark(app_name: str = "BankingLakehouse") -> SparkSession:
    """Get existing SparkSession or create a new one with default settings."""
    return create_spark_session(app_name=app_name)
