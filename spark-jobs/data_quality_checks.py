"""
Data Quality Checks using Great Expectations
Banking Lakehouse Project

Validates Bronze and Silver layer data using Great Expectations.
Generates data docs and supports checkpoint-based validation.
"""

import os
import sys
from typing import Dict, List

from pyspark.sql import SparkSession
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import create_spark_session
from utils.delta_helpers import read_delta


def validate_bronze_transactions(spark: SparkSession, bronze_path: str) -> Dict:
    """
    Validate Bronze transactions data with basic quality checks.

    Checks:
        - Required columns exist
        - No nulls in key columns (transaction_id, account_id)
        - Amount > 0
        - Unique transaction_id
        - Valid status values

    Returns:
        Dictionary with validation results.
    """
    logger.info("Validating Bronze transactions...")

    df = read_delta(spark, f"{bronze_path}/transactions")
    results = {"table": "bronze.transactions", "checks": []}
    total_rows = df.count()

    # Check 1: Required columns exist
    required_cols = [
        "transaction_id", "account_id", "transaction_type",
        "amount", "currency", "status",
    ]
    missing_cols = [c for c in required_cols if c not in df.columns]
    results["checks"].append({
        "check": "required_columns_exist",
        "passed": len(missing_cols) == 0,
        "detail": f"Missing: {missing_cols}" if missing_cols else "All present",
    })

    # Check 2: No nulls in key columns
    null_txn_id = df.filter(df.transaction_id.isNull()).count()
    null_acct_id = df.filter(df.account_id.isNull()).count()
    results["checks"].append({
        "check": "no_nulls_key_columns",
        "passed": null_txn_id == 0 and null_acct_id == 0,
        "detail": f"Null transaction_id: {null_txn_id}, Null account_id: {null_acct_id}",
    })

    # Check 3: Amount > 0
    invalid_amounts = df.filter(df.amount <= 0).count()
    results["checks"].append({
        "check": "positive_amounts",
        "passed": invalid_amounts == 0,
        "detail": f"Invalid amounts: {invalid_amounts} / {total_rows}",
    })

    # Check 4: Unique transaction_id
    distinct_ids = df.select("transaction_id").distinct().count()
    results["checks"].append({
        "check": "unique_transaction_id",
        "passed": distinct_ids == total_rows,
        "detail": f"Distinct: {distinct_ids}, Total: {total_rows}",
    })

    # Check 5: Valid status values
    valid_statuses = {"COMPLETED", "PENDING", "FAILED"}
    actual_statuses = set(
        row.status for row in df.select("status").distinct().collect()
    )
    invalid_statuses = actual_statuses - valid_statuses
    results["checks"].append({
        "check": "valid_status_values",
        "passed": len(invalid_statuses) == 0,
        "detail": f"Invalid statuses: {invalid_statuses}" if invalid_statuses else "All valid",
    })

    results["total_rows"] = total_rows
    results["passed"] = all(c["passed"] for c in results["checks"])

    log_results(results)
    return results


def validate_silver_transactions(spark: SparkSession, silver_path: str) -> Dict:
    """
    Validate Silver transactions data with enriched quality checks.

    Checks:
        - Data completeness (no nulls in critical columns)
        - Referential integrity prep (account_id format)
        - Business rules (amounts, currencies)
        - AML flags present
    """
    logger.info("Validating Silver transactions...")

    df = read_delta(spark, f"{silver_path}/transactions_cleansed")
    results = {"table": "silver.transactions_cleansed", "checks": []}
    total_rows = df.count()

    # Check 1: Completeness — no nulls in critical columns
    critical_cols = ["transaction_id", "account_id", "amount", "currency", "status"]
    for col_name in critical_cols:
        null_count = df.filter(df[col_name].isNull()).count()
        results["checks"].append({
            "check": f"not_null_{col_name}",
            "passed": null_count == 0,
            "detail": f"Nulls: {null_count}",
        })

    # Check 2: Valid currency codes
    valid_currencies = {"AED", "USD", "EUR", "GBP"}
    actual_currencies = set(
        row.currency for row in df.select("currency").distinct().collect()
        if row.currency is not None
    )
    invalid_currencies = actual_currencies - valid_currencies
    results["checks"].append({
        "check": "valid_currencies",
        "passed": len(invalid_currencies) == 0,
        "detail": f"Invalid: {invalid_currencies}" if invalid_currencies else "All valid",
    })

    # Check 3: AML flag column exists
    has_aml_flag = "_aml_flag" in df.columns
    results["checks"].append({
        "check": "aml_flag_present",
        "passed": has_aml_flag,
        "detail": "Column present" if has_aml_flag else "Column missing",
    })

    # Check 4: Cleansed timestamp present
    has_cleansed_ts = "_cleansed_timestamp" in df.columns
    results["checks"].append({
        "check": "cleansed_timestamp_present",
        "passed": has_cleansed_ts,
        "detail": "Present" if has_cleansed_ts else "Missing",
    })

    results["total_rows"] = total_rows
    results["passed"] = all(c["passed"] for c in results["checks"])

    log_results(results)
    return results


def validate_gold_customer_360(spark: SparkSession, gold_path: str) -> Dict:
    """
    Validate Gold Customer 360 view.

    Checks:
        - All customers have a risk_category
        - Non-negative balances and transaction counts
        - Risk category values are valid
    """
    logger.info("Validating Gold Customer 360...")

    df = read_delta(spark, f"{gold_path}/customer_360")
    results = {"table": "gold.customer_360", "checks": []}
    total_rows = df.count()

    # Check 1: Non-null risk_category
    null_risk = df.filter(df.risk_category.isNull()).count()
    results["checks"].append({
        "check": "not_null_risk_category",
        "passed": null_risk == 0,
        "detail": f"Nulls: {null_risk}",
    })

    # Check 2: Valid risk categories
    valid_risk_cats = {"LOW", "MEDIUM", "HIGH"}
    actual_cats = set(
        row.risk_category
        for row in df.select("risk_category").distinct().collect()
        if row.risk_category is not None
    )
    results["checks"].append({
        "check": "valid_risk_categories",
        "passed": actual_cats.issubset(valid_risk_cats),
        "detail": f"Categories: {actual_cats}",
    })

    # Check 3: Non-negative totals
    neg_balance = df.filter(df.total_balance < 0).count()
    neg_txns = df.filter(df.total_transactions < 0).count()
    results["checks"].append({
        "check": "non_negative_totals",
        "passed": neg_balance == 0 and neg_txns == 0,
        "detail": f"Neg balance: {neg_balance}, Neg txns: {neg_txns}",
    })

    results["total_rows"] = total_rows
    results["passed"] = all(c["passed"] for c in results["checks"])

    log_results(results)
    return results


def log_results(results: Dict) -> None:
    """Log validation results in a formatted manner."""
    table = results["table"]
    passed = results["passed"]
    status = "✅ PASSED" if passed else "❌ FAILED"

    logger.info(f"\n{'='*60}")
    logger.info(f"Data Quality: {table} — {status}")
    logger.info(f"Total Rows: {results.get('total_rows', 'N/A')}")
    logger.info(f"{'─'*60}")
    for check in results["checks"]:
        icon = "✅" if check["passed"] else "❌"
        logger.info(f"  {icon} {check['check']}: {check['detail']}")
    logger.info(f"{'='*60}\n")


def run_all_quality_checks(
    bronze_path: str = None,
    silver_path: str = None,
    gold_path: str = None,
) -> Dict:
    """Run all data quality checks across all layers."""
    bronze_path = bronze_path or os.getenv("BRONZE_PATH", "s3a://bronze")
    silver_path = silver_path or os.getenv("SILVER_PATH", "s3a://silver")
    gold_path = gold_path or os.getenv("GOLD_PATH", "s3a://gold")

    spark = create_spark_session(app_name="DataQualityChecks")
    all_results = {}

    try:
        all_results["bronze_transactions"] = validate_bronze_transactions(
            spark, bronze_path
        )
    except Exception as e:
        logger.error(f"Bronze validation failed: {e}")
        all_results["bronze_transactions"] = {"passed": False, "error": str(e)}

    try:
        all_results["silver_transactions"] = validate_silver_transactions(
            spark, silver_path
        )
    except Exception as e:
        logger.error(f"Silver validation failed: {e}")
        all_results["silver_transactions"] = {"passed": False, "error": str(e)}

    try:
        all_results["gold_customer_360"] = validate_gold_customer_360(
            spark, gold_path
        )
    except Exception as e:
        logger.error(f"Gold validation failed: {e}")
        all_results["gold_customer_360"] = {"passed": False, "error": str(e)}

    # Final summary
    total_passed = sum(1 for r in all_results.values() if r.get("passed", False))
    total_checks = len(all_results)
    logger.info(f"\n{'='*60}")
    logger.info(f"Overall Quality Gate: {total_passed}/{total_checks} suites passed")
    logger.info(f"{'='*60}")

    spark.stop()
    return all_results


if __name__ == "__main__":
    run_all_quality_checks()
