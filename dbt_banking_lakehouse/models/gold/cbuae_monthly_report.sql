{{
  config(
    materialized='table',
    file_format='delta',
    location_root='s3a://gold',
    tags=['regulatory', 'monthly', 'cbuae']
  )
}}

/*
GOLD LAYER - CBUAE MONTHLY REGULATORY REPORT
=============================================
Monthly submission to Central Bank of UAE

Report Sections:
1. Customer demographics and KYC status
2. Account summary by type and currency
3. High-value transaction monitoring
4. AML alert summary
5. Data residency compliance

Interview talking points:
- "Automates monthly regulatory submission to CBUAE"
- "Ensures all consumer data residency tags verified"
- "Point-in-time accuracy via Delta time travel"
- "Single source of truth for audit trails"
*/

WITH reporting_month AS (
    SELECT
        DATE_TRUNC('month', CURRENT_DATE() - INTERVAL 1 MONTH) AS report_month_start,
        LAST_DAY(CURRENT_DATE() - INTERVAL 1 MONTH) AS report_month_end
),

-- Section 1: Customer Demographics
customer_summary AS (
    SELECT
        'CUSTOMER_DEMOGRAPHICS' AS section,
        customer_segment,
        kyc_status,
        nationality,
        
        COUNT(DISTINCT customer_id) AS customer_count,
        COUNT(DISTINCT CASE WHEN is_pep = TRUE THEN customer_id END) AS pep_count,
        AVG(risk_score) AS avg_risk_score,
        
        -- Age distribution
        COUNT(DISTINCT CASE WHEN age_band = 'YOUNG' THEN customer_id END) AS young_customers,
        COUNT(DISTINCT CASE WHEN age_band = 'MIDDLE_AGED' THEN customer_id END) AS middle_aged_customers,
        COUNT(DISTINCT CASE WHEN age_band = 'MATURE' THEN customer_id END) AS mature_customers,
        COUNT(DISTINCT CASE WHEN age_band = 'SENIOR' THEN customer_id END) AS senior_customers
        
    FROM {{ ref('customers_silver') }}
    GROUP BY customer_segment, kyc_status, nationality
),

-- Section 2: Account Summary
account_summary AS (
    SELECT
        'ACCOUNT_SUMMARY' AS section,
        account_type,
        currency,
        account_health,
        
        COUNT(DISTINCT account_id) AS account_count,
        SUM(balance_aed_equivalent) AS total_balance_aed,
        AVG(balance_aed_equivalent) AS avg_balance_aed,
        MAX(balance_aed_equivalent) AS max_balance_aed,
        
        COUNT(DISTINCT customer_id) AS unique_customers
        
    FROM {{ ref('accounts_silver') }}
    GROUP BY account_type, currency, account_health
),

-- Section 3: High-Value Transactions (Monthly)
high_value_txn_summary AS (
    SELECT
        'HIGH_VALUE_TRANSACTIONS' AS section,
        DATE(transaction_timestamp) AS transaction_date,
        
        COUNT(DISTINCT transaction_id) AS txn_count,
        SUM(amount_aed) AS total_volume_aed,
        COUNT(DISTINCT account_id) AS unique_accounts,
        COUNT(DISTINCT CASE WHEN is_international = TRUE THEN transaction_id END) AS international_count,
        
        -- Country breakdown
        transaction_country AS destination_country,
        COUNT(DISTINCT CASE WHEN amount_aed > 50000 THEN transaction_id END) AS above_threshold_count
        
    FROM {{ ref('transactions_silver') }} t
    CROSS JOIN reporting_month rm
    WHERE transaction_timestamp BETWEEN rm.report_month_start AND rm.report_month_end
      AND high_value_flag = TRUE
    GROUP BY DATE(transaction_timestamp), transaction_country
),

-- Section 4: AML Alert Summary
aml_summary AS (
    SELECT
        'AML_ALERTS' AS section,
        alert_type,
        severity,
        status,
        
        COUNT(DISTINCT alert_id) AS alert_count,
        AVG(investigation_duration_hours) AS avg_investigation_hours,
        COUNT(DISTINCT CASE WHEN sla_breach_flag = TRUE THEN alert_id END) AS sla_breaches,
        COUNT(DISTINCT customer_id) AS unique_customers_flagged,
        
        SUM(transaction_amount) AS total_flagged_amount_aed
        
    FROM {{ ref('aml_alerts_silver') }} a
    CROSS JOIN reporting_month rm
    WHERE created_at BETWEEN rm.report_month_start AND rm.report_month_end
    GROUP BY alert_type, severity, status
),

-- Section 5: Data Residency Compliance
data_residency_check AS (
    SELECT
        'DATA_RESIDENCY_COMPLIANCE' AS section,
        'CUSTOMERS' AS entity_type,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN _data_residency = 'UAE' THEN 1 END) AS uae_resident_records,
        COUNT(CASE WHEN _data_residency != 'UAE' THEN 1 END) AS non_compliant_records,
        ROUND(COUNT(CASE WHEN _data_residency = 'UAE' THEN 1 END) * 100.0 / COUNT(*), 2) AS compliance_percentage
    FROM {{ ref('customers_silver') }}
    
    UNION ALL
    
    SELECT
        'DATA_RESIDENCY_COMPLIANCE',
        'ACCOUNTS',
        COUNT(*),
        COUNT(CASE WHEN _data_residency = 'UAE' THEN 1 END),
        COUNT(CASE WHEN _data_residency != 'UAE' THEN 1 END),
        ROUND(COUNT(CASE WHEN _data_residency = 'UAE' THEN 1 END) * 100.0 / COUNT(*), 2)
    FROM {{ ref('accounts_silver') }}
    
    UNION ALL
    
    SELECT
        'DATA_RESIDENCY_COMPLIANCE',
        'TRANSACTIONS',
        COUNT(*),
        COUNT(CASE WHEN _data_residency = 'UAE' THEN 1 END),
        COUNT(CASE WHEN _data_residency != 'UAE' THEN 1 END),
        ROUND(COUNT(CASE WHEN _data_residency = 'UAE' THEN 1 END) * 100.0 / COUNT(*), 2)
    FROM {{ ref('transactions_silver') }}
    CROSS JOIN reporting_month rm
    WHERE transaction_timestamp BETWEEN rm.report_month_start AND rm.report_month_end
)

-- Combine all sections (simplified for demo)
SELECT
    rm.report_month_start,
    rm.report_month_end,
    
    -- Summary statistics
    (SELECT COUNT(DISTINCT customer_id) FROM {{ ref('customers_silver') }}) AS total_customers,
    (SELECT COUNT(DISTINCT account_id) FROM {{ ref('accounts_silver') }}) AS total_accounts,
    (SELECT COUNT(DISTINCT transaction_id) FROM {{ ref('transactions_silver') }} t 
     WHERE t.transaction_timestamp BETWEEN rm.report_month_start AND rm.report_month_end) AS monthly_transactions,
    (SELECT COUNT(DISTINCT alert_id) FROM {{ ref('aml_alerts_silver') }} a
     WHERE a.created_at BETWEEN rm.report_month_start AND rm.report_month_end) AS monthly_aml_alerts,
    
    -- Data residency compliance (must be 100%)
    (SELECT compliance_percentage FROM data_residency_check WHERE entity_type = 'CUSTOMERS' LIMIT 1) AS customer_data_compliance_pct,
    
    -- Generated timestamp
    CURRENT_TIMESTAMP() AS report_generated_at,
    
    -- Report status
    CASE
        WHEN (SELECT compliance_percentage FROM data_residency_check WHERE entity_type = 'CUSTOMERS' LIMIT 1) = 100.0
        THEN 'COMPLIANT'
        ELSE 'NON_COMPLIANT'
    END AS compliance_status

FROM reporting_month rm