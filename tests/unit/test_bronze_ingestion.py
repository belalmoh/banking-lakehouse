"""
Unit Tests: Bronze Ingestion
Banking Lakehouse Project
"""

import pytest
from pyspark.sql import functions as F


class TestBronzeIngestion:
    """Test suite for Bronze layer ingestion logic."""

    def test_audit_columns_added(self, spark, sample_transactions_df):
        """Verify audit columns are added during ingestion."""
        from bronze_ingestion import add_audit_columns

        df = add_audit_columns(
            sample_transactions_df,
            source_file="test_transactions.csv",
            data_residency="UAE",
        )

        assert "_ingestion_timestamp" in df.columns
        assert "_source_file" in df.columns
        assert "_batch_id" in df.columns
        assert "_data_classification" in df.columns
        assert "_data_residency" in df.columns

    def test_source_file_column_value(self, spark, sample_transactions_df):
        """Verify source file is correctly recorded."""
        from bronze_ingestion import add_audit_columns

        df = add_audit_columns(
            sample_transactions_df,
            source_file="transactions.csv",
        )

        source_files = [
            row._source_file
            for row in df.select("_source_file").distinct().collect()
        ]
        assert "transactions.csv" in source_files

    def test_data_residency_tag(self, spark, sample_transactions_df):
        """Verify data residency is tagged correctly."""
        from bronze_ingestion import add_audit_columns

        df = add_audit_columns(
            sample_transactions_df,
            source_file="test.csv",
            data_residency="UAE",
        )

        residencies = [
            row._data_residency
            for row in df.select("_data_residency").distinct().collect()
        ]
        assert "UAE" in residencies

    def test_row_count_preserved(self, spark, sample_transactions_df):
        """Verify no rows are lost during audit column addition."""
        from bronze_ingestion import add_audit_columns

        original_count = sample_transactions_df.count()
        df = add_audit_columns(
            sample_transactions_df,
            source_file="test.csv",
        )
        assert df.count() == original_count

    def test_schema_has_all_required_fields(self, spark):
        """Verify schema definitions include required fields."""
        from bronze_ingestion import SCHEMAS

        assert "customers" in SCHEMAS
        assert "accounts" in SCHEMAS
        assert "transactions" in SCHEMAS
        assert "aml_alerts" in SCHEMAS

        # Check transactions schema has key fields
        txn_field_names = [f.name for f in SCHEMAS["transactions"].fields]
        assert "transaction_id" in txn_field_names
        assert "account_id" in txn_field_names
        assert "amount" in txn_field_names
