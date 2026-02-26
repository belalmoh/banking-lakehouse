"""
Data Classification Utilities
Banking Lakehouse Project

Implements PII detection, data classification tagging, and residency
tracking for CBUAE regulatory compliance.
"""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType
from loguru import logger


# ─── PII Classification Mappings ─────────────────────────────────────────────

# Columns considered PII across domains
PII_COLUMNS = {
    "first_name", "last_name", "email", "phone", "date_of_birth",
    "national_id", "passport_number", "address", "ip_address",
}

# Columns considered confidential (business-sensitive but not PII)
CONFIDENTIAL_COLUMNS = {
    "balance", "amount", "risk_score", "kyc_status", "salary",
    "credit_limit", "loan_amount",
}

# High-risk countries for AML screening (FATF grey/black list examples)
HIGH_RISK_COUNTRIES = {"IR", "KP", "MM", "SY", "YE", "AF"}

# AML thresholds
AML_HIGH_VALUE_THRESHOLD_AED = 50_000


def classify_columns(df: DataFrame) -> dict:
    """
    Classify DataFrame columns into PII, CONFIDENTIAL, or PUBLIC.

    Args:
        df: Input DataFrame to classify.

    Returns:
        Dictionary mapping each column to its classification level.
    """
    classifications = {}
    for col_name in df.columns:
        col_lower = col_name.lower()
        if col_lower in PII_COLUMNS:
            classifications[col_name] = "PII"
        elif col_lower in CONFIDENTIAL_COLUMNS:
            classifications[col_name] = "CONFIDENTIAL"
        else:
            classifications[col_name] = "PUBLIC"
    return classifications


def add_classification_columns(
    df: DataFrame,
    data_residency: str = "UAE",
) -> DataFrame:
    """
    Add data governance columns to a DataFrame for regulatory compliance.

    Adds:
        - _data_classification: Overall classification level (PII > CONFIDENTIAL > PUBLIC)
        - _data_residency: Country/region where data is permitted to reside
        - _pii_fields: JSON array of column names classified as PII

    Args:
        df: Input DataFrame.
        data_residency: Data residency region (default: UAE).

    Returns:
        DataFrame with added classification columns.
    """
    classifications = classify_columns(df)

    # Determine overall classification (highest level present)
    pii_fields = [col for col, level in classifications.items() if level == "PII"]
    has_pii = len(pii_fields) > 0
    has_confidential = any(
        level == "CONFIDENTIAL" for level in classifications.values()
    )

    if has_pii:
        overall_classification = "PII"
    elif has_confidential:
        overall_classification = "CONFIDENTIAL"
    else:
        overall_classification = "PUBLIC"

    logger.info(
        f"Data classification: {overall_classification}, "
        f"PII fields: {pii_fields}, residency: {data_residency}"
    )

    # Add governance columns
    df = df.withColumn("_data_classification", F.lit(overall_classification))
    df = df.withColumn("_data_residency", F.lit(data_residency))
    df = df.withColumn("_pii_fields", F.lit(str(pii_fields)))

    return df


def flag_aml_transactions(df: DataFrame) -> DataFrame:
    """
    Flag transactions that require AML (Anti-Money Laundering) review.

    Criteria:
        1. Amount > AED 50,000
        2. International transfer to a high-risk country
        3. Customer risk score > 80

    Args:
        df: Transactions DataFrame with columns: amount, currency,
            is_international, country, risk_score (optional).

    Returns:
        DataFrame with '_aml_flag' and '_aml_reason' columns added.
    """
    high_risk_list = F.array(*[F.lit(c) for c in HIGH_RISK_COUNTRIES])

    # Flag 1: High-value transaction
    high_value = (F.col("amount") > AML_HIGH_VALUE_THRESHOLD_AED) & (
        F.col("currency") == "AED"
    )

    # Flag 2: International to high-risk country
    high_risk_destination = (F.col("is_international") == True) & (  # noqa: E712
        F.col("country").isin(list(HIGH_RISK_COUNTRIES))
    )

    # Flag 3: High-risk customer (if risk_score column exists)
    has_risk_score = "risk_score" in df.columns
    if has_risk_score:
        high_risk_customer = F.col("risk_score") > 80
    else:
        high_risk_customer = F.lit(False)

    # Combine flags
    aml_flag = high_value | high_risk_destination | high_risk_customer

    # Build reason string
    reason = F.concat_ws(
        "; ",
        F.when(high_value, F.lit("HIGH_VALUE_TRANSACTION")).otherwise(F.lit(None)),
        F.when(high_risk_destination, F.lit("HIGH_RISK_COUNTRY")).otherwise(
            F.lit(None)
        ),
        F.when(high_risk_customer, F.lit("HIGH_RISK_CUSTOMER")).otherwise(
            F.lit(None)
        ),
    )

    df = df.withColumn("_aml_flag", aml_flag)
    df = df.withColumn("_aml_reason", F.when(aml_flag, reason).otherwise(F.lit(None)))

    flagged_count = df.filter(F.col("_aml_flag")).count()
    logger.info(f"AML flagging complete: {flagged_count} transactions flagged")

    return df


def hash_pii(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Hash PII columns using SHA-256 for privacy protection.

    Args:
        df: Input DataFrame.
        columns: List of column names to hash.

    Returns:
        DataFrame with specified columns hashed.
    """
    for col_name in columns:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.sha2(F.col(col_name).cast("string"), 256))
            logger.info(f"Hashed PII column: {col_name}")
    return df
