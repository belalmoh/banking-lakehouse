"""
Unit Tests: Data Quality Validation
Banking Lakehouse Project
"""

import pytest
from pyspark.sql import functions as F


class TestDataQuality:
    """Test suite for data quality validation logic."""

    def test_data_classification_columns(self, spark, sample_customers_df):
        """Test that classification columns are correctly added."""
        from utils.data_classification import add_classification_columns

        df = add_classification_columns(sample_customers_df, data_residency="UAE")

        assert "_data_classification" in df.columns
        assert "_data_residency" in df.columns
        assert "_pii_fields" in df.columns

        # Customers contain PII (first_name, last_name, email, etc.)
        classifications = [
            row._data_classification
            for row in df.select("_data_classification").distinct().collect()
        ]
        assert "PII" in classifications

    def test_data_residency_value(self, spark, sample_customers_df):
        """Test data residency value is correct."""
        from utils.data_classification import add_classification_columns

        df = add_classification_columns(sample_customers_df, data_residency="SA")

        residencies = [
            row._data_residency
            for row in df.select("_data_residency").distinct().collect()
        ]
        assert "SA" in residencies

    def test_aml_flagging_high_value(self, spark, sample_transactions_df):
        """Test AML flagging for high-value transactions."""
        from utils.data_classification import flag_aml_transactions

        df = flag_aml_transactions(sample_transactions_df)

        # The 75000 AED transaction should be flagged
        high_value_flagged = df.filter(
            (F.col("amount") == 75000.0) & (F.col("_aml_flag") == True)  # noqa: E712
        ).count()
        assert high_value_flagged > 0

    def test_aml_flagging_small_amounts_not_flagged(self, spark, sample_transactions_df):
        """Test that small transactions are not AML flagged."""
        from utils.data_classification import flag_aml_transactions

        df = flag_aml_transactions(sample_transactions_df)

        # The 150.50 AED transaction should NOT be flagged (unless international to high-risk)
        small_txn = df.filter(F.col("amount") == 150.50).first()
        assert small_txn._aml_flag is False

    def test_pii_hashing(self, spark, sample_customers_df):
        """Test PII columns are hashed correctly."""
        from utils.data_classification import hash_pii

        df = hash_pii(sample_customers_df, columns=["email"])

        # Hashed email should be 64 chars (SHA-256 hex)
        email_val = df.select("email").first().email
        assert len(email_val) == 64  # SHA-256 produces 64 hex chars

    def test_classify_columns_mapping(self, spark, sample_customers_df):
        """Test column classification mapping."""
        from utils.data_classification import classify_columns

        classifications = classify_columns(sample_customers_df)

        assert classifications["first_name"] == "PII"
        assert classifications["email"] == "PII"
        assert classifications["risk_score"] == "CONFIDENTIAL"
        assert classifications["customer_id"] == "PUBLIC"
