"""
Integration Tests: End-to-End Pipeline
Banking Lakehouse Project

Tests the complete data flow from source → Bronze → Silver → Gold.
Requires Spark (uses local mode via conftest fixture).
"""

import os
import pytest
from pyspark.sql import functions as F


class TestEndToEndPipeline:
    """Integration test suite for the complete medallion pipeline."""

    def test_bronze_to_silver_flow(self, spark, sample_transactions_df, tmp_path):
        """Test data flows correctly from Bronze to Silver layer."""
        from utils.delta_helpers import write_delta, read_delta
        from bronze_ingestion import add_audit_columns
        from silver_transformations import cleanse_transactions

        bronze_path = str(tmp_path / "bronze" / "transactions")
        silver_path = str(tmp_path / "silver" / "transactions_cleansed")

        # Simulate Bronze ingestion
        df_with_audit = add_audit_columns(
            sample_transactions_df,
            source_file="test.csv",
            data_residency="UAE",
        )
        write_delta(df_with_audit, bronze_path, mode="overwrite")

        # Read Bronze and apply Silver transformations
        bronze_df = read_delta(spark, bronze_path)
        silver_df = cleanse_transactions(bronze_df)
        write_delta(silver_df, silver_path, mode="overwrite")

        # Verify Silver output
        result = read_delta(spark, silver_path)

        # Should have fewer records (invalid amounts filtered)
        assert result.count() <= sample_transactions_df.count()

        # Should have cleansing columns
        assert "_cleansed_timestamp" in result.columns
        assert "_aml_flag" in result.columns

        # All amounts should be positive
        assert result.filter(F.col("amount") <= 0).count() == 0

    def test_silver_to_gold_customer_360(self, spark, tmp_path):
        """Test Customer 360 Gold aggregation from Silver tables."""
        from utils.delta_helpers import write_delta, read_delta
        from gold_aggregations import build_customer_360

        # Create minimal Silver tables
        customers_data = [
            ("CUST-001", "John", "Doe", "RETAIL", "VERIFIED", 45, "US", "AE",
             "2023-01-01", True),
        ]
        customers_cols = [
            "customer_id", "first_name", "last_name", "customer_segment",
            "kyc_status", "risk_score", "nationality", "country",
            "onboarding_date", "email_valid",
        ]
        customers_df = spark.createDataFrame(customers_data, customers_cols)

        accounts_data = [
            ("ACC-001", "CUST-001", "SAVINGS", "AED", 50000.0, "ACTIVE", "2023-01-01"),
        ]
        accounts_cols = [
            "account_id", "customer_id", "account_type", "currency",
            "balance", "status", "opened_date",
        ]
        accounts_df = spark.createDataFrame(accounts_data, accounts_cols)

        txn_data = [
            ("TXN-001", "ACC-001", "DEBIT", 1000.0, "AED", False, "2024-01-01 10:00:00"),
            ("TXN-002", "ACC-001", "CREDIT", 2000.0, "AED", False, "2024-01-02 11:00:00"),
        ]
        txn_cols = [
            "transaction_id", "account_id", "transaction_type", "amount",
            "currency", "is_international", "transaction_timestamp",
        ]
        txn_df = spark.createDataFrame(txn_data, txn_cols)

        aml_data = [
            ("AML-001", "TXN-001", "ACC-001", "HIGH_VALUE", "MEDIUM", "NEW",
             "2024-01-01 10:00:00", "Officer1"),
        ]
        aml_cols = [
            "alert_id", "transaction_id", "account_id", "alert_type",
            "severity", "status", "created_at", "assigned_to",
        ]
        aml_df = spark.createDataFrame(aml_data, aml_cols)

        # Build Customer 360
        c360 = build_customer_360(customers_df, accounts_df, txn_df, aml_df)

        assert c360.count() == 1
        row = c360.first()
        assert row.customer_id == "CUST-001"
        assert row.total_accounts == 1
        assert row.total_transactions == 2
        assert row.aml_alert_count == 1
        assert row.risk_category in ["LOW", "MEDIUM", "HIGH"]

    def test_data_lineage_preserved(self, spark, sample_transactions_df, tmp_path):
        """Test that audit/lineage columns are preserved through pipeline."""
        from bronze_ingestion import add_audit_columns
        from utils.delta_helpers import write_delta, read_delta

        bronze_path = str(tmp_path / "lineage_test")

        df = add_audit_columns(
            sample_transactions_df,
            source_file="lineage_source.csv",
            data_residency="UAE",
        )
        write_delta(df, bronze_path, mode="overwrite")

        result = read_delta(spark, bronze_path)

        # Verify lineage columns survive Delta round-trip
        assert "_ingestion_timestamp" in result.columns
        assert "_source_file" in result.columns
        assert "_data_residency" in result.columns

        source = result.select("_source_file").first()._source_file
        assert source == "lineage_source.csv"
