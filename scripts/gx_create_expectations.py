"""
Great Expectations - Create Expectation Suites
===============================================
Defines data quality rules for each lakehouse layer.

This script creates expectation suites WITHOUT needing a live Spark connection.
Suites are saved as JSON files and validated at runtime inside Airflow/Docker.

Interview talking points:
- "Expectations are versioned and testable data quality contracts"
- "Failed expectations block bad data from propagating downstream"
- "Auto-generated documentation shows data quality to business users"
"""

import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
import logging
import os

logging.basicConfig(level=logging.INFO)

# ============================================================================
# INITIALIZE CONTEXT (no Spark needed — just file-based)
# ============================================================================

# Change working directory to project root so GX finds gx/ folder
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

context = gx.get_context()

print("=" * 80)
print("GREAT EXPECTATIONS - EXPECTATION SUITE CREATION")
print("=" * 80)


def create_suite(name: str, expectations: list[dict]) -> None:
    """Create an expectation suite with the given expectations."""
    suite = context.add_or_update_expectation_suite(expectation_suite_name=name)
    for exp in expectations:
        meta = exp.pop("meta", {})
        exp_type = exp.pop("expectation_type")
        suite.add_expectation(
            ExpectationConfiguration(
                expectation_type=exp_type,
                kwargs=exp,
                meta=meta,
            )
        )
    context.update_expectation_suite(expectation_suite=suite)
    print(f"  ✅ {name} — {len(expectations)} expectations")


# ============================================================================
# BRONZE LAYER - CUSTOMERS
# ============================================================================

print("\n[1/8] Bronze Customers...")
create_suite("bronze.customers.critical", [
    {
        "expectation_type": "expect_table_row_count_to_be_between",
        "min_value": 10000,
        "meta": {"notes": "Minimum 10K customers expected from source system", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "customer_id",
        "meta": {"notes": "Primary key - cannot be null", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "column": "customer_id",
        "meta": {"notes": "Primary key - must be unique", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "email",
        "meta": {"notes": "Email required for customer communication", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_match_regex",
        "column": "email",
        "regex": r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        "meta": {"notes": "Email must be valid format", "severity": "HIGH"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "kyc_status",
        "value_set": ["VERIFIED", "PENDING", "REJECTED", "EXPIRED"],
        "meta": {"notes": "KYC status must be valid enum value", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "column": "risk_score",
        "min_value": 1,
        "max_value": 100,
        "meta": {"notes": "Risk score range validation", "severity": "HIGH"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "_data_residency",
        "value_set": ["UAE"],
        "meta": {"notes": "CRITICAL: All customer data must reside in UAE for CBUAE compliance", "severity": "CRITICAL", "regulatory": "CBUAE"},
    },
])

# ============================================================================
# BRONZE LAYER - TRANSACTIONS
# ============================================================================

print("[2/8] Bronze Transactions...")
create_suite("bronze.transactions.critical", [
    {
        "expectation_type": "expect_table_row_count_to_be_between",
        "min_value": 100000,
        "meta": {"notes": "Minimum 100K transactions expected", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "transaction_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "column": "transaction_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "amount",
        "meta": {"notes": "Transaction amount is required", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "column": "amount",
        "min_value": 0,
        "meta": {"notes": "Transaction amounts cannot be negative", "severity": "CRITICAL", "business_rule": "BR-001"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "currency",
        "value_set": ["AED", "USD", "EUR", "GBP", "SAR", "INR"],
        "meta": {"notes": "Only supported currencies allowed", "severity": "HIGH"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "status",
        "value_set": ["COMPLETED", "PENDING", "FAILED"],
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "status",
        "value_set": ["COMPLETED"],
        "mostly": 0.90,
        "meta": {"notes": "At least 90% of transactions should complete successfully", "severity": "MEDIUM"},
    },
])

# ============================================================================
# SILVER LAYER - CUSTOMERS
# ============================================================================

print("[3/8] Silver Customers...")
create_suite("silver.customers.critical", [
    {
        "expectation_type": "expect_table_row_count_to_be_between",
        "min_value": 10000,
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "customer_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "column": "customer_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "column": "age_years",
        "min_value": 18,
        "max_value": 120,
        "meta": {"notes": "Customers must be 18+ years old", "severity": "CRITICAL", "business_rule": "BR-003"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "age_band",
        "value_set": ["YOUNG", "MIDDLE_AGED", "MATURE", "SENIOR"],
        "meta": {"notes": "Age band must be properly derived", "severity": "HIGH"},
    },
    {
        "expectation_type": "expect_column_values_to_be_between",
        "column": "risk_score",
        "min_value": 1,
        "max_value": 100,
        "meta": {"notes": "Risk score range validation", "severity": "HIGH"},
    },
])

# ============================================================================
# SILVER LAYER - TRANSACTIONS
# ============================================================================

print("[4/8] Silver Transactions...")
create_suite("silver.transactions.critical", [
    {
        "expectation_type": "expect_table_row_count_to_be_between",
        "min_value": 95000,
        "meta": {"notes": "Max 5% data loss acceptable in Silver transformation", "severity": "HIGH"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "transaction_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "column": "transaction_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_to_exist",
        "column": "high_value_flag",
        "meta": {"notes": "AML high value flag must be computed in Silver", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "time_of_day",
        "value_set": ["OVERNIGHT", "MORNING", "AFTERNOON", "EVENING"],
        "meta": {"severity": "HIGH"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "day_type",
        "value_set": ["WEEKDAY", "WEEKEND"],
        "meta": {"severity": "HIGH"},
    },
])

# ============================================================================
# SILVER LAYER - AML ALERTS
# ============================================================================

print("[5/8] Silver AML Alerts...")
create_suite("silver.aml_alerts.critical", [
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "alert_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "column": "alert_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "severity",
        "value_set": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
        "meta": {"notes": "Alert severity must be valid", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "status",
        "value_set": ["NEW", "INVESTIGATING", "CLEARED", "ESCALATED", "CLOSED"],
        "meta": {"notes": "Alert status must be valid", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_to_exist",
        "column": "sla_breach_flag",
        "meta": {"notes": "SLA breach flag must be computed for compliance monitoring", "severity": "CRITICAL", "regulatory": "CBUAE"},
    },
    {
        "expectation_type": "expect_column_to_exist",
        "column": "sla_hours",
        "meta": {"severity": "CRITICAL"},
    },
])

# ============================================================================
# GOLD LAYER - CUSTOMER 360
# ============================================================================

print("[6/8] Gold Customer 360...")
create_suite("gold.customer_360.critical", [
    {
        "expectation_type": "expect_table_row_count_to_be_between",
        "min_value": 10000,
        "meta": {"notes": "Customer 360 should have one row per customer", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "customer_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_unique",
        "column": "customer_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_to_exist",
        "column": "total_engagement_score",
        "meta": {"notes": "Engagement score required for marketing campaigns", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "engagement_tier",
        "value_set": ["HIGHLY_ENGAGED", "MODERATELY_ENGAGED", "LOW_ENGAGEMENT", "INACTIVE"],
        "meta": {"severity": "HIGH"},
    },
    {
        "expectation_type": "expect_column_to_exist",
        "column": "estimated_annual_revenue_aed",
        "meta": {"notes": "Revenue estimate required for CLV analysis", "severity": "HIGH"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "churn_risk",
        "value_set": ["HIGH_CHURN_RISK", "MEDIUM_CHURN_RISK", "LOW_CHURN_RISK"],
        "meta": {"notes": "Churn risk required for retention campaigns", "severity": "HIGH"},
    },
])

# ============================================================================
# GOLD LAYER - AML DASHBOARD
# ============================================================================

print("[7/8] Gold AML Dashboard...")
create_suite("gold.aml_dashboard.critical", [
    {
        "expectation_type": "expect_column_values_to_not_be_null",
        "column": "alert_id",
        "meta": {"severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_to_exist",
        "column": "priority_rank",
        "meta": {"notes": "Priority rank required for analyst assignment", "severity": "CRITICAL"},
    },
    {
        "expectation_type": "expect_column_to_exist",
        "column": "composite_risk_score",
        "meta": {"notes": "Composite risk score for holistic risk assessment", "severity": "HIGH"},
    },
    {
        "expectation_type": "expect_column_values_to_be_in_set",
        "column": "sla_status",
        "value_set": ["COMPLETED", "OVERDUE", "AT_RISK", "ON_TRACK"],
        "meta": {"notes": "SLA status for compliance monitoring", "severity": "CRITICAL", "regulatory": "CBUAE"},
    },
])

# ============================================================================
# CROSS-LAYER REFERENTIAL INTEGRITY
# ============================================================================

print("[8/8] Cross-layer referential integrity...")
context.add_or_update_expectation_suite(
    expectation_suite_name="cross_layer.referential_integrity"
)
print("  ✅ cross_layer.referential_integrity — placeholder")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "=" * 80)
print("✅ ALL EXPECTATION SUITES CREATED!")
print("=" * 80)
print("\nExpectation Suites:")
print("  1. bronze.customers.critical")
print("  2. bronze.transactions.critical")
print("  3. silver.customers.critical")
print("  4. silver.transactions.critical")
print("  5. silver.aml_alerts.critical")
print("  6. gold.customer_360.critical")
print("  7. gold.aml_dashboard.critical")
print("  8. cross_layer.referential_integrity")
print("\nSuites saved to: gx/expectations/")
print("Next: Create checkpoints and integrate with Airflow")