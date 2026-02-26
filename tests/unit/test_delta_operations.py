"""
Unit Tests: Delta Lake Operations
Banking Lakehouse Project
"""

import pytest
from pyspark.sql import functions as F


class TestDeltaOperations:
    """Test suite for Delta Lake helper operations."""

    def test_write_and_read_delta(self, spark, sample_transactions_df, temp_delta_path):
        """Test basic Delta write and read round-trip."""
        from utils.delta_helpers import write_delta, read_delta

        write_delta(sample_transactions_df, temp_delta_path, mode="overwrite")
        df_read = read_delta(spark, temp_delta_path)

        assert df_read.count() == sample_transactions_df.count()
        assert set(df_read.columns) == set(sample_transactions_df.columns)

    def test_append_mode(self, spark, sample_transactions_df, temp_delta_path):
        """Test Delta append mode adds records."""
        from utils.delta_helpers import write_delta, read_delta

        # Write first batch
        write_delta(sample_transactions_df, temp_delta_path, mode="overwrite")
        initial_count = read_delta(spark, temp_delta_path).count()

        # Append second batch
        write_delta(sample_transactions_df, temp_delta_path, mode="append")
        final_count = read_delta(spark, temp_delta_path).count()

        assert final_count == initial_count * 2

    def test_table_exists(self, spark, sample_transactions_df, temp_delta_path):
        """Test table existence check."""
        from utils.delta_helpers import write_delta, table_exists

        # Should not exist before write
        assert table_exists(spark, temp_delta_path) is False

        # Should exist after write
        write_delta(sample_transactions_df, temp_delta_path, mode="overwrite")
        assert table_exists(spark, temp_delta_path) is True

    def test_upsert_delta_creates_new_table(
        self, spark, sample_transactions_df, temp_delta_path
    ):
        """Test upsert creates a new table if target doesn't exist."""
        from utils.delta_helpers import upsert_delta, read_delta

        upsert_delta(
            spark,
            sample_transactions_df,
            temp_delta_path,
            merge_key="transaction_id",
        )

        result = read_delta(spark, temp_delta_path)
        assert result.count() == sample_transactions_df.count()

    def test_table_history(self, spark, sample_transactions_df, temp_delta_path):
        """Test Delta table version history."""
        from utils.delta_helpers import write_delta, get_table_history

        # Create two versions
        write_delta(sample_transactions_df, temp_delta_path, mode="overwrite")
        write_delta(sample_transactions_df, temp_delta_path, mode="append")

        history = get_table_history(spark, temp_delta_path, limit=5)
        assert history.count() >= 2
