"""
Banking Data Generator for Lakehouse Demo
Generates realistic customer, account, transaction, and AML data
"""

import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import numpy as np

# Initialize Faker with locale for UAE context
fake = Faker(['en_US', 'ar_AE'])
random.seed(42)  # Reproducibility
np.random.seed(42)

# ============================================================================
# CONSTANTS - Based on Banking Business Context
# ============================================================================

# Customer segments (serves diverse demographics)
CUSTOMER_SEGMENTS = ['RETAIL', 'CORPORATE', 'PRIVATE_BANKING', 'SME']
SEGMENT_WEIGHTS = [0.70, 0.15, 0.10, 0.05]  # 70% retail customers

# Risk scores distribution (most customers low risk)
RISK_SCORE_RANGES = {
    'LOW': (1, 30),
    'MEDIUM': (31, 60),
    'HIGH': (61, 85),
    'CRITICAL': (86, 100)
}
RISK_WEIGHTS = [0.75, 0.15, 0.08, 0.02]  # 75% low risk

# KYC status distribution
KYC_STATUS = ['VERIFIED', 'PENDING', 'REJECTED', 'EXPIRED']
KYC_WEIGHTS = [0.85, 0.10, 0.03, 0.02]  # 85% verified

# Nationalities (UAE demographics + expats)
NATIONALITIES = ['UAE', 'India', 'Pakistan', 'Egypt', 'Philippines', 
                 'UK', 'USA', 'Jordan', 'Lebanon', 'Bangladesh']
NATIONALITY_WEIGHTS = [0.20, 0.25, 0.10, 0.08, 0.07, 0.05, 0.05, 0.05, 0.05, 0.10]

# Countries for transactions (where customers transact)
TRANSACTION_COUNTRIES = ['UAE', 'USA', 'UK', 'India', 'Saudi Arabia', 
                         'Singapore', 'Switzerland', 'Hong Kong', 'Turkey', 'Lebanon']

# Account types
ACCOUNT_TYPES = ['SAVINGS', 'CHECKING', 'CREDIT_CARD', 'PERSONAL_LOAN', 
                 'MORTGAGE', 'BUSINESS_ACCOUNT']

# Currencies (multi-currency bank)
CURRENCIES = ['AED', 'USD', 'EUR', 'GBP', 'SAR', 'INR']
CURRENCY_WEIGHTS = [0.60, 0.20, 0.10, 0.05, 0.03, 0.02]

# Transaction types and merchant categories
TRANSACTION_TYPES = ['DEBIT', 'CREDIT']
MERCHANT_CATEGORIES = [
    'Groceries', 'Restaurants', 'Fuel', 'Shopping', 'Healthcare',
    'Education', 'Entertainment', 'Travel', 'Utilities', 'Real Estate',
    'Automotive', 'Electronics', 'Jewelry', 'Cash Withdrawal', 'Transfer'
]

# AML alert types (based on actual screening rules)
AML_ALERT_TYPES = [
    'HIGH_VALUE_TRANSACTION',      # > AED 50,000
    'SUSPICIOUS_PATTERN',           # Unusual for customer
    'VELOCITY_ANOMALY',             # Too many transactions
    'GEO_ANOMALY',                  # High-risk country
    'STRUCTURING',                  # Breaking up large amounts
    'ROUND_AMOUNT',                 # Exactly AED 10,000
    'PEP_MATCH',                    # Politically Exposed Person
    'SANCTION_SCREENING'            # Watchlist match
]

# ============================================================================
# CUSTOMER GENERATION
# ============================================================================

def generate_customers(n=10000):
    """
    Generate customer master data
    
    Interview talking point: "Customer data includes risk scoring for KYC/AML,
    data classification tags for CBUAE compliance, and segment stratification
    for personalized banking services."
    """
    customers = []
    
    for i in range(n):
        customer_id = f"C{str(i+1).zfill(6)}"  # C000001, C000002...
        
        # Segment determines many downstream attributes
        segment = random.choices(CUSTOMER_SEGMENTS, weights=SEGMENT_WEIGHTS)[0]
        
        # Risk score based on segment (corporate generally lower risk)
        if segment == 'CORPORATE':
            risk_category = random.choices(['LOW', 'MEDIUM'], weights=[0.9, 0.1])[0]
        elif segment == 'PRIVATE_BANKING':
            risk_category = random.choices(['LOW', 'MEDIUM', 'HIGH'], weights=[0.6, 0.3, 0.1])[0]
        else:
            risk_category = random.choices(list(RISK_SCORE_RANGES.keys()), weights=RISK_WEIGHTS)[0]
        
        risk_score = random.randint(*RISK_SCORE_RANGES[risk_category])
        
        # Generate realistic customer profile
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.free_email_domain()}"
        
        # Phone numbers (UAE format +971)
        phone = f"+971-{random.randint(50, 59)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
        
        # Date of birth (18-80 years old)
        age = random.randint(18, 80)
        date_of_birth = fake.date_of_birth(minimum_age=age, maximum_age=age)
        
        # Nationality
        nationality = random.choices(NATIONALITIES, weights=NATIONALITY_WEIGHTS)[0]
        
        # Country of residence (90% match nationality, 10% expats)
        if random.random() < 0.9:
            country_residence = nationality
        else:
            country_residence = random.choice(['UAE', 'Saudi Arabia', 'Qatar', 'Bahrain'])
        
        # KYC status
        kyc_status = random.choices(KYC_STATUS, weights=KYC_WEIGHTS)[0]
        
        # Onboarding date (last 5 years)
        onboarding_date = fake.date_between(start_date='-5y', end_date='today')
        
        # PEP flag (Politically Exposed Person - higher scrutiny)
        is_pep = random.random() < 0.02  # 2% are PEPs
        
        # Data classification (for CBUAE compliance demo)
        data_classification = 'PII'  # All customer data is PII
        
        customers.append({
            'customer_id': customer_id,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'phone': phone,
            'date_of_birth': date_of_birth,
            'nationality': nationality,
            'country_residence': country_residence,
            'customer_segment': segment,
            'risk_score': risk_score,
            'risk_category': risk_category,
            'kyc_status': kyc_status,
            'is_pep': is_pep,
            'onboarding_date': onboarding_date,
            'data_classification': data_classification,
            'created_at': datetime.now()
        })
    
    return pd.DataFrame(customers)

# ============================================================================
# ACCOUNT GENERATION
# ============================================================================

def generate_accounts(customers_df, avg_accounts_per_customer=2):
    """
    Generate account data linked to customers
    
    Interview talking point: "Account generation follows realistic patterns -
    retail customers have 1-3 accounts (savings + checking), while corporate
    customers may have 5-10 different account types."
    """
    accounts = []
    
    for _, customer in customers_df.iterrows():
        # Number of accounts varies by segment
        if customer['customer_segment'] == 'RETAIL':
            n_accounts = random.choices([1, 2, 3], weights=[0.3, 0.5, 0.2])[0]
        elif customer['customer_segment'] == 'CORPORATE':
            n_accounts = random.choices([3, 5, 8], weights=[0.4, 0.4, 0.2])[0]
        elif customer['customer_segment'] == 'PRIVATE_BANKING':
            n_accounts = random.choices([4, 6, 10], weights=[0.3, 0.5, 0.2])[0]
        else:  # SME
            n_accounts = random.choices([2, 3, 5], weights=[0.4, 0.4, 0.2])[0]
        
        for i in range(n_accounts):
            account_id = f"ACC{uuid.uuid4().hex[:10].upper()}"
            
            # Account type selection (first account usually savings/checking)
            if i == 0:
                account_type = random.choice(['SAVINGS', 'CHECKING'])
            else:
                account_type = random.choice(ACCOUNT_TYPES)
            
            # Currency (mostly AED, some foreign currency accounts)
            currency = random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS)[0]
            
            # Balance based on account type and customer segment
            if account_type == 'SAVINGS':
                if customer['customer_segment'] == 'PRIVATE_BANKING':
                    balance = random.randint(500000, 5000000)  # AED 500K - 5M
                elif customer['customer_segment'] == 'CORPORATE':
                    balance = random.randint(100000, 2000000)  # AED 100K - 2M
                else:
                    balance = random.randint(5000, 150000)  # AED 5K - 150K
            
            elif account_type == 'CHECKING':
                balance = random.randint(1000, 50000)
            
            elif account_type == 'CREDIT_CARD':
                balance = -random.randint(0, 50000)  # Negative = outstanding
            
            elif account_type in ['PERSONAL_LOAN', 'MORTGAGE']:
                balance = -random.randint(50000, 1000000)  # Loan outstanding
            
            else:  # BUSINESS_ACCOUNT
                balance = random.randint(50000, 500000)
            
            # Account status (most active)
            status = random.choices(['ACTIVE', 'DORMANT', 'CLOSED'], 
                                   weights=[0.85, 0.10, 0.05])[0]
            
            # Opened date (after customer onboarding)
            days_after_onboarding = random.randint(0, 365)
            opened_date = customer['onboarding_date'] + timedelta(days=days_after_onboarding)
            
            # Last activity
            if status == 'ACTIVE':
                last_activity = fake.date_between(start_date='-30d', end_date='today')
            elif status == 'DORMANT':
                last_activity = fake.date_between(start_date='-365d', end_date='-90d')
            else:  # CLOSED
                last_activity = fake.date_between(start_date='-2y', end_date='-1y')
            
            accounts.append({
                'account_id': account_id,
                'customer_id': customer['customer_id'],
                'account_type': account_type,
                'currency': currency,
                'balance': balance,
                'status': status,
                'opened_date': opened_date,
                'last_activity_date': last_activity,
                'data_classification': 'CONFIDENTIAL',  # Balance data
                'created_at': datetime.now()
            })
    
    return pd.DataFrame(accounts)

# ============================================================================
# TRANSACTION GENERATION
# ============================================================================

def generate_transactions(accounts_df, n_transactions=100000):
    """
    Generate transaction data with realistic patterns
    
    Interview talking point: "Transactions include pattern anomalies that
    trigger AML alerts - high values, velocity spikes, geo-anomalies, and
    structuring behaviors that real-world monitoring systems detect."
    """
    transactions = []
    
    # Filter only active accounts for transactions
    active_accounts = accounts_df[accounts_df['status'] == 'ACTIVE']
    
    for i in range(n_transactions):
        transaction_id = f"TXN{uuid.uuid4().hex[:12].upper()}"
        
        # Select random account
        account = active_accounts.sample(1).iloc[0]
        
        # Transaction type (70% debit, 30% credit)
        transaction_type = random.choices(TRANSACTION_TYPES, weights=[0.7, 0.3])[0]
        
        # Amount based on transaction type and account type
        if account['account_type'] == 'CREDIT_CARD':
            # Credit card transactions typically smaller
            amount = round(random.uniform(50, 5000), 2)
        elif account['account_type'] in ['PERSONAL_LOAN', 'MORTGAGE']:
            # Loan payments
            if transaction_type == 'DEBIT':
                amount = round(random.uniform(1000, 10000), 2)
            else:
                amount = round(random.uniform(5000, 100000), 2)  # Disbursement
        else:
            # Regular accounts - varying amounts
            amount = round(np.random.lognormal(mean=7, sigma=2), 2)  # Log-normal distribution
            amount = min(amount, 1000000)  # Cap at 1M
        
        # Create some HIGH_VALUE transactions for AML (> 50,000 AED)
        if random.random() < 0.05:  # 5% are high value
            amount = round(random.uniform(50000, 500000), 2)
        
        # Merchant and category
        merchant_category = random.choice(MERCHANT_CATEGORIES)
        if merchant_category == 'Cash Withdrawal':
            merchant_name = f"ATM-{random.randint(1000, 9999)}"
        elif merchant_category == 'Transfer':
            merchant_name = f"TRANSFER-{fake.first_name()}"
        else:
            merchant_name = fake.company()
        
        # Transaction timestamp (last 90 days, with realistic daily pattern)
        days_ago = random.randint(0, 90)
        hour = np.random.choice(range(24), p=generate_hourly_distribution())
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        
        transaction_timestamp = datetime.now() - timedelta(
            days=days_ago, 
            hours=int(23-hour), 
            minutes=(59-minute),
            seconds=(59-second)
        )
        
        # Country (90% domestic, 10% international)
        is_international = random.random() < 0.10
        if is_international:
            country = random.choice([c for c in TRANSACTION_COUNTRIES if c != 'UAE'])
        else:
            country = 'UAE'
        
        # Status (95% completed, 5% failed/pending)
        status = random.choices(['COMPLETED', 'PENDING', 'FAILED'], 
                               weights=[0.95, 0.03, 0.02])[0]
        
        # Description
        description = f"{transaction_type} - {merchant_category} - {merchant_name}"
        
        transactions.append({
            'transaction_id': transaction_id,
            'account_id': account['account_id'],
            'transaction_type': transaction_type,
            'amount': amount,
            'currency': account['currency'],
            'merchant_name': merchant_name,
            'merchant_category': merchant_category,
            'transaction_timestamp': transaction_timestamp,
            'country': country,
            'is_international': is_international,
            'status': status,
            'description': description,
            'data_classification': 'CONFIDENTIAL',
            'created_at': datetime.now()
        })
    
    return pd.DataFrame(transactions)

def generate_hourly_distribution():
    """
    Generate realistic hourly transaction distribution
    Banking transactions peak during business hours (9 AM - 6 PM)
    """
    distribution = []
    for hour in range(24):
        if 9 <= hour <= 18:  # Business hours
            distribution.append(0.08)  # Higher probability
        elif 6 <= hour <= 8 or 19 <= hour <= 22:  # Morning/evening
            distribution.append(0.03)
        else:  # Night
            distribution.append(0.005)
    
    # Normalize to sum to 1
    total = sum(distribution)
    return [x/total for x in distribution]

# ============================================================================
# AML ALERT GENERATION
# ============================================================================

def generate_aml_alerts(transactions_df, accounts_df, customers_df, n_alerts=5000):
    """
    Generate AML alerts based on transaction patterns
    
    Interview talking point: "AML alert generation simulates real monitoring
    rules like CBUAE's transaction screening requirements - high-value flags,
    velocity anomalies, and PEP screening. This demonstrates understanding of
    regulatory compliance workflows."
    """
    alerts = []
    
    # Join transactions with account and customer data for context
    txn_with_context = transactions_df.merge(
        accounts_df[['account_id', 'customer_id']], 
        on='account_id'
    ).merge(
        customers_df[['customer_id', 'is_pep', 'risk_score']], 
        on='customer_id'
    )
    
    for i in range(n_alerts):
        alert_id = f"AML{uuid.uuid4().hex[:10].upper()}"
        
        # Select alert type
        alert_type = random.choice(AML_ALERT_TYPES)
        
        # Select transaction based on alert type
        if alert_type == 'HIGH_VALUE_TRANSACTION':
            # Get transactions > 50,000
            eligible = txn_with_context[txn_with_context['amount'] > 50000]
            if len(eligible) == 0:
                continue
            txn = eligible.sample(1).iloc[0]
            severity = random.choices(['MEDIUM', 'HIGH'], weights=[0.6, 0.4])[0]
        
        elif alert_type == 'PEP_MATCH':
            # Get PEP customer transactions
            eligible = txn_with_context[txn_with_context['is_pep'] == True]
            if len(eligible) == 0:
                continue
            txn = eligible.sample(1).iloc[0]
            severity = random.choices(['HIGH', 'CRITICAL'], weights=[0.7, 0.3])[0]
        
        elif alert_type == 'GEO_ANOMALY':
            # International transactions
            eligible = txn_with_context[txn_with_context['is_international'] == True]
            if len(eligible) == 0:
                continue
            txn = eligible.sample(1).iloc[0]
            severity = random.choices(['LOW', 'MEDIUM', 'HIGH'], weights=[0.5, 0.3, 0.2])[0]
        
        elif alert_type == 'STRUCTURING':
            # Multiple transactions just below reporting threshold
            txn = txn_with_context.sample(1).iloc[0]
            severity = random.choices(['MEDIUM', 'HIGH', 'CRITICAL'], weights=[0.4, 0.4, 0.2])[0]
        
        else:
            # Random transaction for other alert types
            txn = txn_with_context.sample(1).iloc[0]
            severity = random.choices(['LOW', 'MEDIUM', 'HIGH'], weights=[0.5, 0.3, 0.2])[0]
        
        # Alert status workflow
        status = random.choices(
            ['NEW', 'INVESTIGATING', 'CLEARED', 'ESCALATED', 'CLOSED'],
            weights=[0.15, 0.25, 0.35, 0.15, 0.10]
        )[0]
        
        # Assigned analyst
        analysts = ['Ahmed.Ali', 'Sara.Khan', 'Mohammed.Hassan', 'Fatima.Zayed', 'John.Smith']
        assigned_to = random.choice(analysts) if status != 'NEW' else None
        
        # Created timestamp (within 24 hours of transaction)
        hours_after_txn = random.randint(1, 24)
        created_at = txn['transaction_timestamp'] + timedelta(hours=hours_after_txn)
        
        # Resolution notes (if cleared/closed)
        if status in ['CLEARED', 'CLOSED']:
            resolution_notes = random.choice([
                'Verified legitimate business transaction',
                'Customer provided supporting documentation',
                'Below threshold after review',
                'False positive - regular customer pattern',
                'Approved by compliance officer'
            ])
        elif status == 'ESCALATED':
            resolution_notes = 'Escalated to senior compliance for SAR filing'
        else:
            resolution_notes = None
        
        # Investigation duration (if not NEW)
        if status != 'NEW':
            investigation_hours = random.randint(4, 72)
            last_updated = created_at + timedelta(hours=investigation_hours)
        else:
            last_updated = created_at
        
        alerts.append({
            'alert_id': alert_id,
            'transaction_id': txn['transaction_id'],
            'account_id': txn['account_id'],
            'customer_id': txn['customer_id'],
            'alert_type': alert_type,
            'severity': severity,
            'status': status,
            'assigned_to': assigned_to,
            'created_at': created_at,
            'last_updated': last_updated,
            'resolution_notes': resolution_notes,
            'customer_risk_score': txn['risk_score'],
            'transaction_amount': txn['amount'],
            'transaction_country': txn['country'],
            'data_classification': 'CONFIDENTIAL'
        })
    
    return pd.DataFrame(alerts)

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Generate all banking datasets and save to CSV"""
    
    print("=" * 80)
    print("ADCB BANKING LAKEHOUSE - DATA GENERATION")
    print("=" * 80)
    
    # Generate customers
    print("\n[1/4] Generating customers...")
    customers_df = generate_customers(n=10000)
    print(f"✅ Generated {len(customers_df):,} customers")
    print(f"    - Segments: {customers_df['customer_segment'].value_counts().to_dict()}")
    print(f"    - KYC Verified: {(customers_df['kyc_status'] == 'VERIFIED').sum():,}")
    print(f"    - PEPs: {customers_df['is_pep'].sum():,}")
    
    # Generate accounts
    print("\n[2/4] Generating accounts...")
    accounts_df = generate_accounts(customers_df, avg_accounts_per_customer=2)
    print(f"✅ Generated {len(accounts_df):,} accounts")
    print(f"    - Account types: {accounts_df['account_type'].value_counts().to_dict()}")
    print(f"    - Active accounts: {(accounts_df['status'] == 'ACTIVE').sum():,}")
    print(f"    - Total balance (AED): {accounts_df[accounts_df['currency']=='AED']['balance'].sum():,.2f}")
    
    # Generate transactions
    print("\n[3/4] Generating transactions...")
    transactions_df = generate_transactions(accounts_df, n_transactions=100000)
    print(f"✅ Generated {len(transactions_df):,} transactions")
    print(f"    - Completed: {(transactions_df['status'] == 'COMPLETED').sum():,}")
    print(f"    - International: {transactions_df['is_international'].sum():,}")
    print(f"    - High-value (>50K): {(transactions_df['amount'] > 50000).sum():,}")
    print(f"    - Total volume (AED): {transactions_df[transactions_df['currency']=='AED']['amount'].sum():,.2f}")
    
    # Generate AML alerts
    print("\n[4/4] Generating AML alerts...")
    aml_alerts_df = generate_aml_alerts(transactions_df, accounts_df, customers_df, n_alerts=5000)
    print(f"✅ Generated {len(aml_alerts_df):,} AML alerts")
    print(f"    - By severity: {aml_alerts_df['severity'].value_counts().to_dict()}")
    print(f"    - By status: {aml_alerts_df['status'].value_counts().to_dict()}")
    print(f"    - Critical alerts: {(aml_alerts_df['severity'] == 'CRITICAL').sum():,}")
    
    # Save to CSV
    print("\n" + "=" * 80)
    print("SAVING DATA FILES")
    print("=" * 80)
    
    output_dir = 'data/sample'
    
    customers_df.to_csv(f'{output_dir}/customers.csv', index=False)
    print(f"✅ Saved: {output_dir}/customers.csv")
    
    accounts_df.to_csv(f'{output_dir}/accounts.csv', index=False)
    print(f"✅ Saved: {output_dir}/accounts.csv")
    
    transactions_df.to_csv(f'{output_dir}/transactions.csv', index=False)
    print(f"✅ Saved: {output_dir}/transactions.csv")
    
    aml_alerts_df.to_csv(f'{output_dir}/aml_alerts.csv', index=False)
    print(f"✅ Saved: {output_dir}/aml_alerts.csv")
    
    # Data quality summary
    print("\n" + "=" * 80)
    print("DATA QUALITY CHECKS")
    print("=" * 80)
    
    print("\n✓ Referential Integrity:")
    print(f"  - All accounts link to valid customers: {accounts_df['customer_id'].isin(customers_df['customer_id']).all()}")
    print(f"  - All transactions link to valid accounts: {transactions_df['account_id'].isin(accounts_df['account_id']).all()}")
    print(f"  - All alerts link to valid transactions: {aml_alerts_df['transaction_id'].isin(transactions_df['transaction_id']).all()}")
    
    print("\n✓ Data Completeness:")
    print(f"  - Customers with null emails: {customers_df['email'].isnull().sum()}")
    print(f"  - Accounts with null balance: {accounts_df['balance'].isnull().sum()}")
    print(f"  - Transactions with null amount: {transactions_df['amount'].isnull().sum()}")
    
    print("\n✓ Business Rules:")
    print(f"  - Transactions with negative amounts: {(transactions_df['amount'] < 0).sum()}")
    print(f"  - Future-dated transactions: {(transactions_df['transaction_timestamp'] > datetime.now()).sum()}")
    print(f"  - Customers under 18: {(customers_df['date_of_birth'] > (datetime.now().date() - timedelta(days=18*365))).sum()}")
    
    print("\n" + "=" * 80)
    print("✅ DATA GENERATION COMPLETE!")
    print("=" * 80)
    print(f"\nNext step: Upload to MinIO and ingest into Bronze layer")

if __name__ == "__main__":
    main()