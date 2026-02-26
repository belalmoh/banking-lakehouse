"""
Delta Lake Helper Operations
Banking Lakehouse Project

Provides reusable utilities for Delta Lake read/write, merge (upsert),
time travel, vacuum, and schema evolution operations.
"""

from datetime import datetime
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from loguru import logger


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "append",
    partition_by: Optional[List[str]] = None,
    merge_schema: bool = True,
) -> None:
    """
    Write a DataFrame to Delta Lake.

    Args:
        df: Source DataFrame.
        path: Delta table path (e.g., s3a://bronze/transactions).
        mode: Write mode — 'append', 'overwrite', or 'error'.
        partition_by: Columns to partition by.
        merge_schema: Allow automatic schema evolution.
    """
    logger.info(f"Writing Delta table: path={path}, mode={mode}, rows={df.count()}")

    writer = df.write.format("delta").mode(mode)

    if merge_schema:
        writer = writer.option("mergeSchema", "true")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    writer.save(path)
    logger.info(f"Delta write complete: {path}")


def read_delta(
    spark: SparkSession,
    path: str,
    version: Optional[int] = None,
    timestamp: Optional[str] = None,
) -> DataFrame:
    """
    Read a Delta table with optional time travel.

    Args:
        spark: Active SparkSession.
        path: Delta table path.
        version: Specific version number for time travel.
        timestamp: Timestamp string for time travel (e.g., '2024-01-01').

    Returns:
        DataFrame from the Delta table.
    """
    reader = spark.read.format("delta")

    if version is not None:
        logger.info(f"Reading Delta table at version {version}: {path}")
        reader = reader.option("versionAsOf", version)
    elif timestamp is not None:
        logger.info(f"Reading Delta table at timestamp {timestamp}: {path}")
        reader = reader.option("timestampAsOf", timestamp)
    else:
        logger.info(f"Reading Delta table (latest): {path}")

    return reader.load(path)


def upsert_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_key: str,
    update_columns: Optional[List[str]] = None,
) -> None:
    """
    Perform an upsert (merge) into a Delta table.

    If the target table does not exist, creates it from the source DataFrame.

    Args:
        spark: Active SparkSession.
        source_df: Source DataFrame with new/updated records.
        target_path: Path to the target Delta table.
        merge_key: Column name used as the join key.
        update_columns: Columns to update on match. If None, updates all columns.
    """
    if DeltaTable.isDeltaTable(spark, target_path):
        logger.info(f"Merging into existing Delta table: {target_path}")
        target_table = DeltaTable.forPath(spark, target_path)

        merge_builder = target_table.alias("target").merge(
            source_df.alias("source"),
            f"target.{merge_key} = source.{merge_key}",
        )

        if update_columns:
            update_set = {col: f"source.{col}" for col in update_columns}
            merge_builder = merge_builder.whenMatchedUpdate(set=update_set)
        else:
            merge_builder = merge_builder.whenMatchedUpdateAll()

        merge_builder.whenNotMatchedInsertAll().execute()
        logger.info(f"Merge complete: {target_path}")
    else:
        logger.info(f"Target table not found. Creating new table: {target_path}")
        write_delta(source_df, target_path, mode="overwrite")


def get_table_history(
    spark: SparkSession, path: str, limit: int = 10
) -> DataFrame:
    """
    Get the version history of a Delta table.

    Args:
        spark: Active SparkSession.
        path: Delta table path.
        limit: Maximum number of history entries.

    Returns:
        DataFrame with version history.
    """
    delta_table = DeltaTable.forPath(spark, path)
    return delta_table.history(limit)


def vacuum_table(
    spark: SparkSession, path: str, retention_hours: int = 168
) -> None:
    """
    Vacuum a Delta table to remove old files.

    Args:
        spark: Active SparkSession.
        path: Delta table path.
        retention_hours: File retention period in hours (default 7 days).
    """
    logger.info(f"Vacuuming Delta table: {path} (retention={retention_hours}h)")
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.vacuum(retention_hours)
    logger.info(f"Vacuum complete: {path}")


def optimize_table(spark: SparkSession, path: str) -> None:
    """
    Optimize a Delta table by compacting small files.

    Args:
        spark: Active SparkSession.
        path: Delta table path.
    """
    logger.info(f"Optimizing Delta table: {path}")
    delta_table = DeltaTable.forPath(spark, path)
    delta_table.optimize().executeCompaction()
    logger.info(f"Optimization complete: {path}")


def table_exists(spark: SparkSession, path: str) -> bool:
    """Check if a Delta table exists at the given path."""
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False
