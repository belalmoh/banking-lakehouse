# CBUAE Compliance Framework

## Overview

This lakehouse implements data governance controls aligned with the Central Bank of the UAE (CBUAE) regulatory requirements for financial institutions.

## Compliance Controls

### 1. Data Classification

Every record is tagged with three governance fields:

| Field | Values | Purpose |
|-------|--------|---------|
| `_data_classification` | PII / CONFIDENTIAL / PUBLIC | Sensitivity level |
| `_data_residency` | UAE / SA / US / GB / IN | Permitted storage region |
| `_pii_fields` | JSON array | Which columns contain PII |

**PII Columns**: first_name, last_name, email, phone, date_of_birth, national_id, passport_number, address

**Confidential Columns**: balance, amount, risk_score, kyc_status, salary, credit_limit, loan_amount

### 2. Data Residency (Sovereign Data)

UAE banking regulations require certain data to remain within UAE jurisdiction:
- Customer PII must be tagged with `_data_residency = "UAE"`
- Regulatory reports include `data_residency` field
- Cross-border data transfers tracked via `is_international` flag

### 3. Anti-Money Laundering (AML)

Transactions are automatically screened against AML criteria:

| Rule | Threshold | Flag |
|------|-----------|------|
| High-value transaction | Amount > AED 50,000 | `HIGH_VALUE` |
| High-risk country | Transfer to FATF list | `GEO_ANOMALY` |
| High-risk customer | Risk score > 80 | Per customer profile |
| Velocity | >10 txns/hour (configurable) | `VELOCITY` |

AML alerts are tracked with:
- Severity levels: LOW, MEDIUM, HIGH, CRITICAL
- Status workflow: NEW → INVESTIGATING → CLEARED / ESCALATED
- Resolution rate tracking in Gold layer

### 4. Know Your Customer (KYC)

Customer verification lifecycle:
- **VERIFIED**: Completed KYC process
- **PENDING**: In-progress verification
- **REJECTED**: Failed verification

Customer segments: RETAIL, CORPORATE, PRIVATE_BANKING

### 5. Privacy-Preserving Reporting

Regulatory reports use SHA-256 hashing for customer identifiers:
- No raw PII in Gold regulatory tables
- Hashed customer IDs maintain referential utility
- Compliant with data minimization principles

### 6. Audit Trails

Every record includes complete provenance:
- `_ingestion_timestamp`: When data entered the system
- `_source_file`: Original source file
- `_batch_id`: Processing batch reference
- `_cleansed_timestamp`: When cleansing was applied
- `_gold_timestamp`: When aggregation was computed

Delta Lake time travel enables:
- Historical data reconstruction at any point in time
- Regulatory audit support (view data as-of a specific date)
- Rollback capability for data corrections

### 7. Regulatory Reporting

The `gold.regulatory_report` table produces CBUAE-ready output:
- Monthly transaction summaries by customer (hashed)
- Risk categorization (LOW/MEDIUM/HIGH)
- KYC verification status
- AML alert exposure
- All timestamps in UTC
- Data residency flagging

## Applicable Regulations

| Regulation | Relevance | Implementation |
|-----------|-----------|---------------|
| CBUAE Circular 2019/01 | AML/CFT compliance | AML flagging, alert tracking |
| UAE Data Protection Law | PII handling | Data classification, residency |
| FATF Recommendations | Risk-based approach | Composite risk scoring |
| Basel III | Capital adequacy | Account balance tracking |
