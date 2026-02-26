# Banking Lakehouse dbt Project

## Overview

This dbt project implements the **Silver** and **Gold** layers of the Banking Lakehouse
medallion architecture. It transforms raw Bronze data into cleansed Silver tables and
business-ready Gold aggregations.

## Architecture

```
Bronze (Raw)  →  Silver (Cleansed)  →  Gold (Business)
─────────────    ────────────────     ────────────────
customers        customers_cleansed   customer_360
accounts         accounts_cleansed    aml_summary
transactions     transactions_cleansed regulatory_report
aml_alerts       aml_alerts_cleansed
```

## Models

### Silver Layer (Incremental)
- **customers_cleansed**: Deduplicated, standardized customer profiles
- **accounts_cleansed**: Validated account records with derived age
- **transactions_cleansed**: Cleaned transactions with AML flags
- **aml_alerts_cleansed**: Standardized alerts with severity scores

### Gold Layer (Full Refresh)
- **customer_360**: Comprehensive customer view with aggregated metrics
- **aml_summary**: AML intelligence by customer and alert type
- **regulatory_report**: CBUAE-ready reporting with hashed identifiers

## Macros
- `convert_currency`: FX conversion between AED/USD/EUR/GBP
- `calculate_risk_score`: Composite risk scoring (KYC + AML + transactions)
- `data_quality_checks`: Reusable test macros for banking validation

## Running

```bash
# Compile models
dbt compile

# Run all models
dbt run

# Run Silver only
dbt run --models silver.*

# Run Gold only
dbt run --models gold.*

# Run tests
dbt test

# Generate documentation
dbt docs generate && dbt docs serve
```
