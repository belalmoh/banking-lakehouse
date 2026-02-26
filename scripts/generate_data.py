"""
Banking Data Generator
Banking Lakehouse Project

Generates realistic banking sample data using the Faker library.
Produces: customers, accounts, transactions, and AML alerts.
Saves as both CSV and JSON formats.
"""

import csv
import json
import os
import random
from datetime import datetime, timedelta
from typing import Dict, List

from faker import Faker
from loguru import logger


# Initialize Faker with seed for reproducibility
fake = Faker()
Faker.seed(42)
random.seed(42)

# Output directory
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "sample")

# ─── Constants ────────────────────────────────────────────────────────────────

COUNTRIES = ["AE", "SA", "US", "GB", "IN"]
COUNTRY_WEIGHTS = [0.4, 0.2, 0.15, 0.15, 0.1]  # UAE-heavy weighting

CURRENCIES = ["AED", "USD", "EUR", "GBP"]
CURRENCY_WEIGHTS = [0.5, 0.25, 0.15, 0.1]

ACCOUNT_TYPES = ["SAVINGS", "CHECKING", "CREDIT_CARD", "LOAN"]
ACCOUNT_TYPE_WEIGHTS = [0.35, 0.35, 0.2, 0.1]

TRANSACTION_TYPES = ["DEBIT", "CREDIT"]
MERCHANT_CATEGORIES = [
    "GROCERY", "RESTAURANT", "FUEL", "RETAIL", "UTILITIES",
    "ENTERTAINMENT", "TRAVEL", "HEALTHCARE", "EDUCATION", "GOVERNMENT",
]

KYC_STATUSES = ["VERIFIED", "PENDING", "REJECTED"]
KYC_WEIGHTS = [0.75, 0.20, 0.05]

CUSTOMER_SEGMENTS = ["RETAIL", "CORPORATE", "PRIVATE_BANKING"]
SEGMENT_WEIGHTS = [0.70, 0.20, 0.10]

ACCOUNT_STATUSES = ["ACTIVE", "INACTIVE", "SUSPENDED"]
ACCOUNT_STATUS_WEIGHTS = [0.85, 0.10, 0.05]

TRANSACTION_STATUSES = ["COMPLETED", "PENDING", "FAILED"]
TXN_STATUS_WEIGHTS = [0.90, 0.07, 0.03]

ALERT_TYPES = ["HIGH_VALUE", "SUSPICIOUS_PATTERN", "VELOCITY", "GEO_ANOMALY"]
ALERT_SEVERITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
ALERT_SEVERITY_WEIGHTS = [0.35, 0.35, 0.20, 0.10]

ALERT_STATUSES = ["NEW", "INVESTIGATING", "CLEARED", "ESCALATED"]
ALERT_STATUS_WEIGHTS = [0.30, 0.30, 0.30, 0.10]

NATIONALITIES = ["Emirati", "Saudi", "American", "British", "Indian",
                  "Pakistani", "Egyptian", "Filipino", "Lebanese", "Jordanian"]


def generate_customers(num_records: int = 10_000) -> List[Dict]:
    """Generate realistic customer records."""
    logger.info(f"Generating {num_records} customers...")
    customers = []

    for i in range(num_records):
        country = random.choices(COUNTRIES, weights=COUNTRY_WEIGHTS, k=1)[0]
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
        onboarding = fake.date_between(start_date="-5y", end_date="today")

        customer = {
            "customer_id": f"CUST-{i+1:06d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "date_of_birth": dob.strftime("%Y-%m-%d"),
            "nationality": random.choice(NATIONALITIES),
            "risk_score": random.randint(1, 100),
            "kyc_status": random.choices(KYC_STATUSES, weights=KYC_WEIGHTS, k=1)[0],
            "onboarding_date": onboarding.strftime("%Y-%m-%d"),
            "customer_segment": random.choices(
                CUSTOMER_SEGMENTS, weights=SEGMENT_WEIGHTS, k=1
            )[0],
            "country": country,
        }
        customers.append(customer)

    logger.info(f"Generated {len(customers)} customers")
    return customers


def generate_accounts(
    customers: List[Dict], avg_accounts_per_customer: float = 2.0
) -> List[Dict]:
    """Generate bank account records linked to customers."""
    num_records = int(len(customers) * avg_accounts_per_customer)
    logger.info(f"Generating ~{num_records} accounts...")
    accounts = []

    account_id = 0
    for customer in customers:
        # Each customer gets 1-4 accounts
        num_accounts = random.choices([1, 2, 3, 4], weights=[0.3, 0.4, 0.2, 0.1], k=1)[0]

        for _ in range(num_accounts):
            account_id += 1
            account_type = random.choices(
                ACCOUNT_TYPES, weights=ACCOUNT_TYPE_WEIGHTS, k=1
            )[0]

            # Balance varies by account type
            if account_type == "SAVINGS":
                balance = round(random.uniform(100, 500_000), 2)
            elif account_type == "CHECKING":
                balance = round(random.uniform(50, 100_000), 2)
            elif account_type == "CREDIT_CARD":
                balance = round(random.uniform(0, 50_000), 2)
            else:  # LOAN
                balance = round(random.uniform(5_000, 1_000_000), 2)

            account = {
                "account_id": f"ACC-{account_id:06d}",
                "customer_id": customer["customer_id"],
                "account_type": account_type,
                "currency": random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS, k=1)[0],
                "balance": balance,
                "status": random.choices(
                    ACCOUNT_STATUSES, weights=ACCOUNT_STATUS_WEIGHTS, k=1
                )[0],
                "opened_date": fake.date_between(start_date="-5y", end_date="today").strftime("%Y-%m-%d"),
            }
            accounts.append(account)

    logger.info(f"Generated {len(accounts)} accounts")
    return accounts


def generate_transactions(
    accounts: List[Dict], num_records: int = 100_000
) -> List[Dict]:
    """Generate realistic transaction records over the last 90 days."""
    logger.info(f"Generating {num_records} transactions...")
    transactions = []

    account_ids = [a["account_id"] for a in accounts]
    now = datetime.now()
    ninety_days_ago = now - timedelta(days=90)

    for i in range(num_records):
        account_id = random.choice(account_ids)
        txn_type = random.choice(TRANSACTION_TYPES)

        # Amount distribution: most are small, some are large
        if random.random() < 0.05:  # 5% high-value transactions
            amount = round(random.uniform(10_000, 200_000), 2)
        elif random.random() < 0.2:  # 20% medium
            amount = round(random.uniform(1_000, 10_000), 2)
        else:  # 75% small
            amount = round(random.uniform(5, 1_000), 2)

        is_international = random.random() < 0.15
        country = random.choice(COUNTRIES) if is_international else "AE"

        # Random timestamp in the last 90 days
        random_seconds = random.randint(0, int((now - ninety_days_ago).total_seconds()))
        txn_timestamp = ninety_days_ago + timedelta(seconds=random_seconds)

        transaction = {
            "transaction_id": f"TXN-{i+1:08d}",
            "account_id": account_id,
            "transaction_type": txn_type,
            "amount": amount,
            "currency": random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS, k=1)[0],
            "merchant_name": fake.company(),
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
            "transaction_timestamp": txn_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "country": country,
            "status": random.choices(
                TRANSACTION_STATUSES, weights=TXN_STATUS_WEIGHTS, k=1
            )[0],
            "is_international": is_international,
        }
        transactions.append(transaction)

    logger.info(f"Generated {len(transactions)} transactions")
    return transactions


def generate_aml_alerts(
    transactions: List[Dict],
    accounts: List[Dict],
    num_records: int = 5_000,
) -> List[Dict]:
    """Generate AML alert records linked to suspicious transactions."""
    logger.info(f"Generating {num_records} AML alerts...")
    alerts = []

    # Focus on higher-value transactions for alert generation
    high_value_txns = [t for t in transactions if t["amount"] > 5_000]
    if len(high_value_txns) < num_records:
        high_value_txns = transactions  # Fallback to all

    sampled_txns = random.sample(high_value_txns, min(num_records, len(high_value_txns)))

    compliance_officers = [fake.name() for _ in range(20)]  # Pool of officers

    for i, txn in enumerate(sampled_txns):
        alert = {
            "alert_id": f"AML-{i+1:06d}",
            "transaction_id": txn["transaction_id"],
            "account_id": txn["account_id"],
            "alert_type": random.choice(ALERT_TYPES),
            "severity": random.choices(
                ALERT_SEVERITIES, weights=ALERT_SEVERITY_WEIGHTS, k=1
            )[0],
            "status": random.choices(
                ALERT_STATUSES, weights=ALERT_STATUS_WEIGHTS, k=1
            )[0],
            "created_at": txn["transaction_timestamp"],
            "assigned_to": random.choice(compliance_officers),
        }
        alerts.append(alert)

    logger.info(f"Generated {len(alerts)} AML alerts")
    return alerts


def save_csv(data: List[Dict], filename: str) -> str:
    """Save data as CSV file."""
    filepath = os.path.join(OUTPUT_DIR, filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    if not data:
        logger.warning(f"No data to save for {filename}")
        return filepath

    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)

    logger.info(f"Saved CSV: {filepath} ({len(data)} records)")
    return filepath


def save_json(data: List[Dict], filename: str) -> str:
    """Save data as JSON file (newline-delimited for Spark compatibility)."""
    filepath = os.path.join(OUTPUT_DIR, filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as f:
        for record in data:
            f.write(json.dumps(record) + "\n")

    logger.info(f"Saved JSON: {filepath} ({len(data)} records)")
    return filepath


def main():
    """Generate all sample banking data."""
    logger.info("=" * 60)
    logger.info("Banking Data Generator — Starting")
    logger.info("=" * 60)

    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Generate data (with referential integrity)
    customers = generate_customers(num_records=10_000)
    accounts = generate_accounts(customers, avg_accounts_per_customer=2.0)
    transactions = generate_transactions(accounts, num_records=100_000)
    aml_alerts = generate_aml_alerts(transactions, accounts, num_records=5_000)

    # Save as CSV
    save_csv(customers, "customers.csv")
    save_csv(accounts, "accounts.csv")
    save_csv(transactions, "transactions.csv")
    save_csv(aml_alerts, "aml_alerts.csv")

    # Save transactions as JSON (alternative format)
    save_json(transactions, "transactions.json")

    # Summary
    logger.info("=" * 60)
    logger.info("Data Generation Summary")
    logger.info("=" * 60)
    logger.info(f"  📊 Customers:    {len(customers):>10,}")
    logger.info(f"  📊 Accounts:     {len(accounts):>10,}")
    logger.info(f"  📊 Transactions: {len(transactions):>10,}")
    logger.info(f"  📊 AML Alerts:   {len(aml_alerts):>10,}")
    logger.info(f"  📁 Output:       {OUTPUT_DIR}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
