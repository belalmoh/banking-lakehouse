"""
Gold Layer Aggregations (PySpark)
Banking Lakehouse Project

Transitional Gold layer aggregations using PySpark.
These will be migrated to dbt models. Produces business-level views:
Customer 360, AML Summary, and Regulatory Reports.
"""

import os
import sys
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.spark_session import create_spark_session
from utils.delta_helpers import read_delta, write_delta
from utils.data_classification import hash_pii


def build_customer_360(
    customers_df: DataFrame,
    accounts_df: DataFrame,
    transactions_df: DataFrame,
    aml_alerts_df: DataFrame,
) -> DataFrame:
    """
    Build Customer 360 view by joining customers with accounts,
    transactions, and AML alerts.

    Output columns:
        - customer_id, customer_name, segment, kyc_status, risk_score
        - total_accounts, total_balance
        - total_transactions, total_debit, total_credit
        - avg_transaction_amount, max_transaction_amount
        - aml_alert_count, latest_aml_severity
        - risk_category (LOW/MEDIUM/HIGH based on risk_score)
    """
    logger.info("Building Customer 360 view...")

    # Aggregate accounts per customer
    accounts_agg = accounts_df.groupBy("customer_id").agg(
        F.count("account_id").alias("total_accounts"),
        F.sum("balance").alias("total_balance"),
        F.collect_set("account_type").alias("account_types"),
    )

    # Aggregate transactions per customer via account_id
    txn_with_customer = transactions_df.join(
        accounts_df.select("account_id", "customer_id"),
        on="account_id",
        how="inner",
    )

    txn_agg = txn_with_customer.groupBy("customer_id").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum(
            F.when(F.col("transaction_type") == "DEBIT", F.col("amount")).otherwise(0)
        ).alias("total_debit"),
        F.sum(
            F.when(F.col("transaction_type") == "CREDIT", F.col("amount")).otherwise(0)
        ).alias("total_credit"),
        F.avg("amount").alias("avg_transaction_amount"),
        F.max("amount").alias("max_transaction_amount"),
    )

    # Aggregate AML alerts per customer
    aml_agg = aml_alerts_df.join(
        accounts_df.select("account_id", "customer_id"),
        on="account_id",
        how="inner",
    ).groupBy("customer_id").agg(
        F.count("alert_id").alias("aml_alert_count"),
        F.max("severity").alias("latest_aml_severity"),
    )

    # Join everything
    customer_360 = (
        customers_df.select(
            "customer_id",
            F.concat_ws(" ", "first_name", "last_name").alias("customer_name"),
            "customer_segment",
            "kyc_status",
            "risk_score",
            "nationality",
            "country",
            "onboarding_date",
        )
        .join(accounts_agg, on="customer_id", how="left")
        .join(txn_agg, on="customer_id", how="left")
        .join(aml_agg, on="customer_id", how="left")
    )

    # Fill nulls
    customer_360 = customer_360.fillna(
        {
            "total_accounts": 0,
            "total_balance": 0.0,
            "total_transactions": 0,
            "total_debit": 0.0,
            "total_credit": 0.0,
            "avg_transaction_amount": 0.0,
            "max_transaction_amount": 0.0,
            "aml_alert_count": 0,
        }
    )

    # Risk categorization
    customer_360 = customer_360.withColumn(
        "risk_category",
        F.when(F.col("risk_score") > 80, F.lit("HIGH"))
        .when(F.col("risk_score") > 40, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )

    # Add metadata
    customer_360 = customer_360.withColumn(
        "_gold_timestamp", F.current_timestamp()
    )

    record_count = customer_360.count()
    logger.info(f"Customer 360 built: {record_count} records")
    return customer_360


def build_aml_summary(
    transactions_df: DataFrame,
    aml_alerts_df: DataFrame,
    accounts_df: DataFrame,
) -> DataFrame:
    """
    Build AML Summary with transaction patterns and alert aggregations.
    """
    logger.info("Building AML Summary...")

    # Enrich alerts with transaction and account data
    aml_enriched = (
        aml_alerts_df
        .join(transactions_df, on="transaction_id", how="left")
        .join(
            accounts_df.select("account_id", "customer_id", "account_type"),
            on="account_id",
            how="left",
        )
    )

    aml_summary = aml_enriched.groupBy(
        "customer_id", "alert_type", "severity"
    ).agg(
        F.count("alert_id").alias("alert_count"),
        F.sum("amount").alias("total_flagged_amount"),
        F.avg("amount").alias("avg_flagged_amount"),
        F.min("created_at").alias("first_alert_date"),
        F.max("created_at").alias("last_alert_date"),
        F.collect_set(F.col("status")).alias("alert_statuses"),
    )

    aml_summary = aml_summary.withColumn("_gold_timestamp", F.current_timestamp())

    record_count = aml_summary.count()
    logger.info(f"AML Summary built: {record_count} records")
    return aml_summary


def build_regulatory_report(
    customers_df: DataFrame,
    accounts_df: DataFrame,
    transactions_df: DataFrame,
    aml_alerts_df: DataFrame,
) -> DataFrame:
    """
    Build CBUAE Regulatory Report with:
    - Hashed customer identifier (privacy)
    - Monthly transaction summaries
    - Risk category
    - AML alert count
    - KYC status
    - All timestamps in UTC
    """
    logger.info("Building Regulatory Report...")

    # Get monthly transaction summary per customer
    txn_with_customer = transactions_df.join(
        accounts_df.select("account_id", "customer_id"),
        on="account_id",
        how="inner",
    )

    monthly_txn = txn_with_customer.withColumn(
        "report_month", F.date_format(F.col("transaction_timestamp"), "yyyy-MM")
    ).groupBy("customer_id", "report_month").agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.sum(
            F.when(F.col("is_international") == True, 1).otherwise(0)  # noqa: E712
        ).alias("international_transaction_count"),
    )

    # AML alert count per customer
    aml_count = aml_alerts_df.join(
        accounts_df.select("account_id", "customer_id"),
        on="account_id",
        how="inner",
    ).groupBy("customer_id").agg(
        F.count("alert_id").alias("aml_alert_count"),
    )

    # Join with customer data
    report = (
        monthly_txn
        .join(
            customers_df.select(
                "customer_id", "kyc_status", "risk_score", "nationality"
            ),
            on="customer_id",
            how="inner",
        )
        .join(aml_count, on="customer_id", how="left")
    )

    report = report.fillna({"aml_alert_count": 0})

    # Risk categorization
    report = report.withColumn(
        "risk_category",
        F.when(F.col("risk_score") > 80, F.lit("HIGH"))
        .when(F.col("risk_score") > 40, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )

    # Hash customer_id for privacy in regulatory report
    report = hash_pii(report, columns=["customer_id"])
    report = report.withColumnRenamed("customer_id", "customer_hash")

    # Add report metadata
    report = report.withColumn("report_generated_at", F.current_timestamp())
    report = report.withColumn("data_residency", F.lit("UAE"))
    report = report.withColumn("regulatory_body", F.lit("CBUAE"))

    record_count = report.count()
    logger.info(f"Regulatory Report built: {record_count} records")
    return report


def run_gold_aggregations(
    silver_path: str = None,
    gold_path: str = None,
) -> dict:
    """
    Execute all Gold layer aggregations.

    Returns:
        Dictionary with record counts per gold table.
    """
    silver_path = silver_path or os.getenv("SILVER_PATH", "s3a://silver")
    gold_path = gold_path or os.getenv("GOLD_PATH", "s3a://gold")

    spark = create_spark_session(app_name="GoldAggregations")
    results = {}

    try:
        # Read Silver tables
        customers = read_delta(spark, f"{silver_path}/customers_cleansed")
        accounts = read_delta(spark, f"{silver_path}/accounts_cleansed")
        transactions = read_delta(spark, f"{silver_path}/transactions_cleansed")
        aml_alerts = read_delta(spark, f"{silver_path}/aml_alerts_cleansed")

        # Customer 360
        c360 = build_customer_360(customers, accounts, transactions, aml_alerts)
        write_delta(c360, f"{gold_path}/customer_360", mode="overwrite")
        results["customer_360"] = c360.count()

        # AML Summary
        aml_sum = build_aml_summary(transactions, aml_alerts, accounts)
        write_delta(aml_sum, f"{gold_path}/aml_summary", mode="overwrite")
        results["aml_summary"] = aml_sum.count()

        # Regulatory Report
        reg_report = build_regulatory_report(
            customers, accounts, transactions, aml_alerts
        )
        write_delta(reg_report, f"{gold_path}/regulatory_reports", mode="overwrite")
        results["regulatory_reports"] = reg_report.count()

    except Exception as e:
        logger.error(f"Gold Aggregation failed: {e}")
        raise

    # Summary
    logger.info("=" * 60)
    logger.info("Gold Aggregation Summary")
    logger.info("=" * 60)
    for table, count in results.items():
        logger.info(f"  ✅ {table}: {count} records")

    spark.stop()
    return results


if __name__ == "__main__":
    run_gold_aggregations()
