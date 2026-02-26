# Data Dictionary

## Bronze Layer (Raw)

### bronze.customers
| Column | Type | Classification | Description |
|--------|------|---------------|-------------|
| customer_id | STRING | PUBLIC | Unique customer identifier (CUST-XXXXXX) |
| first_name | STRING | PII | Customer first name |
| last_name | STRING | PII | Customer last name |
| email | STRING | PII | Email address |
| phone | STRING | PII | Phone number |
| date_of_birth | STRING | PII | Date of birth (YYYY-MM-DD) |
| nationality | STRING | PUBLIC | Customer nationality |
| risk_score | INT | CONFIDENTIAL | KYC risk score (1-100) |
| kyc_status | STRING | PUBLIC | VERIFIED / PENDING / REJECTED |
| onboarding_date | STRING | PUBLIC | Customer onboarding date |
| customer_segment | STRING | PUBLIC | RETAIL / CORPORATE / PRIVATE_BANKING |
| country | STRING | PUBLIC | Country code (AE, SA, US, GB, IN) |
| _ingestion_timestamp | TIMESTAMP | PUBLIC | UTC ingestion time |
| _source_file | STRING | PUBLIC | Source file name |
| _batch_id | STRING | PUBLIC | Ingestion batch identifier |
| _data_classification | STRING | PUBLIC | PII / CONFIDENTIAL / PUBLIC |
| _data_residency | STRING | PUBLIC | Data residency region |
| _pii_fields | STRING | PUBLIC | JSON array of PII column names |

### bronze.accounts
| Column | Type | Classification | Description |
|--------|------|---------------|-------------|
| account_id | STRING | PUBLIC | Unique account identifier (ACC-XXXXXX) |
| customer_id | STRING | PUBLIC | FK to customers |
| account_type | STRING | PUBLIC | SAVINGS / CHECKING / CREDIT_CARD / LOAN |
| currency | STRING | PUBLIC | AED / USD / EUR / GBP |
| balance | DOUBLE | CONFIDENTIAL | Current account balance |
| status | STRING | PUBLIC | ACTIVE / INACTIVE / SUSPENDED |
| opened_date | STRING | PUBLIC | Account opening date |

### bronze.transactions
| Column | Type | Classification | Description |
|--------|------|---------------|-------------|
| transaction_id | STRING | PUBLIC | Unique transaction ID (TXN-XXXXXXXX) |
| account_id | STRING | PUBLIC | FK to accounts |
| transaction_type | STRING | PUBLIC | DEBIT / CREDIT |
| amount | DOUBLE | CONFIDENTIAL | Transaction amount |
| currency | STRING | PUBLIC | Transaction currency |
| merchant_name | STRING | PUBLIC | Merchant name |
| merchant_category | STRING | PUBLIC | GROCERY / RESTAURANT / FUEL / etc. |
| transaction_timestamp | STRING | PUBLIC | Transaction time (YYYY-MM-DD HH:MM:SS) |
| country | STRING | PUBLIC | Country where transaction occurred |
| status | STRING | PUBLIC | COMPLETED / PENDING / FAILED |
| is_international | BOOLEAN | PUBLIC | Cross-border flag |

### bronze.aml_alerts
| Column | Type | Classification | Description |
|--------|------|---------------|-------------|
| alert_id | STRING | PUBLIC | Unique alert ID (AML-XXXXXX) |
| transaction_id | STRING | PUBLIC | Related transaction ID |
| account_id | STRING | PUBLIC | Related account ID |
| alert_type | STRING | PUBLIC | HIGH_VALUE / SUSPICIOUS_PATTERN / VELOCITY / GEO_ANOMALY |
| severity | STRING | PUBLIC | LOW / MEDIUM / HIGH / CRITICAL |
| status | STRING | PUBLIC | NEW / INVESTIGATING / CLEARED / ESCALATED |
| created_at | STRING | PUBLIC | Alert creation timestamp |
| assigned_to | STRING | PUBLIC | Assigned compliance officer |

---

## Silver Layer (Cleansed)

Silver tables inherit all Bronze columns (cleansed) plus:

| Column | Type | Description |
|--------|------|-------------|
| _cleansed_timestamp | TIMESTAMP | When cleansing was applied |
| email_valid | BOOLEAN | Email format validation (customers) |
| risk_category | STRING | LOW / MEDIUM / HIGH (customers) |
| account_age_days | INT | Days since account opened (accounts) |
| transaction_date | DATE | Extracted date (transactions) |
| transaction_hour | INT | Hour of day (transactions) |
| aml_flag | BOOLEAN | AML review required (transactions) |
| severity_score | INT | Numeric severity 1-4 (aml_alerts) |
| alert_age_hours | DOUBLE | Hours since alert created (aml_alerts) |

---

## Gold Layer (Business)

### gold.customer_360
| Column | Type | Description |
|--------|------|-------------|
| customer_id | STRING | Unique customer identifier |
| customer_name | STRING | Full name (first + last) |
| customer_segment | STRING | RETAIL / CORPORATE / PRIVATE_BANKING |
| kyc_status | STRING | KYC verification status |
| risk_score | INT | KYC risk score |
| risk_category | STRING | LOW / MEDIUM / HIGH |
| total_accounts | INT | Number of accounts |
| total_balance | DOUBLE | Sum of all balances |
| total_transactions | INT | Total transaction count |
| total_debit | DOUBLE | Sum of debit amounts |
| total_credit | DOUBLE | Sum of credit amounts |
| avg_transaction_amount | DOUBLE | Average transaction amount |
| aml_alert_count | INT | Number of AML alerts |
| composite_risk_score | DOUBLE | Weighted risk: KYC(40%) + AML(35%) + Txn(25%) |
| _gold_timestamp | TIMESTAMP | Aggregation timestamp |

### gold.aml_summary
| Column | Type | Description |
|--------|------|-------------|
| customer_id | STRING | Associated customer |
| alert_type | STRING | Type of AML alert |
| severity | STRING | Alert severity |
| alert_count | INT | Number of alerts |
| total_flagged_amount | DOUBLE | Sum of flagged amounts |
| resolution_rate_pct | DOUBLE | % of cleared alerts |
| _gold_timestamp | TIMESTAMP | Aggregation timestamp |

### gold.regulatory_report
| Column | Type | Description |
|--------|------|-------------|
| customer_hash | STRING | SHA-256 hashed customer ID |
| report_month | STRING | Reporting period (YYYY-MM) |
| kyc_status | STRING | Customer KYC status |
| risk_category | STRING | LOW / MEDIUM / HIGH |
| transaction_count | INT | Monthly transaction count |
| total_amount | DOUBLE | Monthly total amount |
| aml_alert_count | INT | Total AML alerts |
| regulatory_body | STRING | Always "CBUAE" |
| data_residency | STRING | Always "UAE" |
