"""
Pytest Configuration and Shared Fixtures
Banking Lakehouse Project
"""

import os
import sys
import pytest

# Add spark-jobs to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "spark-jobs"))


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for testing.
    Uses local mode with Delta Lake extensions.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("BankingLakehouseTests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture
def sample_customers_df(spark):
    """Create a sample customers DataFrame for testing."""
    data = [
        ("CUST-000001", "John", "Doe", "john@example.com", "+971501234567",
         "1990-05-15", "American", 45, "VERIFIED", "2023-01-15", "RETAIL", "AE"),
        ("CUST-000002", "Jane", "Smith", "jane@example.com", "+971502345678",
         "1985-10-20", "British", 82, "VERIFIED", "2022-06-01", "PRIVATE_BANKING", "GB"),
        ("CUST-000003", "Ahmed", "Hassan", "ahmed@example.com", "+971503456789",
         "1995-03-08", "Emirati", 25, "PENDING", "2024-01-01", "RETAIL", "AE"),
    ]
    columns = [
        "customer_id", "first_name", "last_name", "email", "phone",
        "date_of_birth", "nationality", "risk_score", "kyc_status",
        "onboarding_date", "customer_segment", "country",
    ]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def sample_transactions_df(spark):
    """Create a sample transactions DataFrame for testing."""
    data = [
        ("TXN-00000001", "ACC-000001", "DEBIT", 150.50, "AED",
         "Carrefour", "GROCERY", "2024-01-15 10:30:00", "AE",
         "COMPLETED", False),
        ("TXN-00000002", "ACC-000001", "CREDIT", 75000.00, "AED",
         "ADCB Transfer", "TRANSFER", "2024-01-15 14:00:00", "AE",
         "COMPLETED", False),
        ("TXN-00000003", "ACC-000002", "DEBIT", 500.00, "USD",
         "Amazon", "RETAIL", "2024-01-16 09:15:00", "US",
         "COMPLETED", True),
        ("TXN-00000004", "ACC-000003", "DEBIT", -50.00, "AED",
         "Invalid", "OTHER", "2024-01-16 11:00:00", "AE",
         "FAILED", False),
    ]
    columns = [
        "transaction_id", "account_id", "transaction_type", "amount", "currency",
        "merchant_name", "merchant_category", "transaction_timestamp", "country",
        "status", "is_international",
    ]
    return spark.createDataFrame(data, columns)


@pytest.fixture
def temp_delta_path(tmp_path):
    """Provide a temporary directory for Delta table testing."""
    return str(tmp_path / "delta_test")
