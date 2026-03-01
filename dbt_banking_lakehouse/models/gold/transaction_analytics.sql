{{
  config(
    materialized='incremental',
    unique_key='analytics_date',
    file_format='delta',
    location_root='s3a://gold',
    incremental_strategy='merge',
    partition_by=['analytics_date']
  )
}}

/*
Gold Layer - Transaction Analytics
===================================
Daily aggregated transaction metrics for business intelligence

Interview points:
- "Incremental materialization with daily grain reduces processing time"
- "Pre-aggregated for Tableau/PowerBI dashboards"
- "Supports trend analysis and forecasting models"
*/

WITH daily_transactions AS (
    SELECT
        transaction_date AS analytics_date,
        
        -- Volume metrics
        COUNT(DISTINCT transaction_id) AS total_transactions,
        COUNT(DISTINCT account_id) AS active_accounts,
        COUNT(DISTINCT CASE WHEN transaction_type = 'DEBIT' THEN transaction_id END) AS debit_count,
        COUNT(DISTINCT CASE WHEN transaction_type = 'CREDIT' THEN transaction_id END) AS credit_count,
        
        -- Value metrics (AED)
        SUM(amount_aed) AS total_volume_aed,
        SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount_aed ELSE 0 END) AS total_debit_aed,
        SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount_aed ELSE 0 END) AS total_credit_aed,
        
        AVG(amount_aed) AS avg_transaction_size_aed,
        PERCENTILE(amount_aed, 0.5) AS median_transaction_size_aed,
        MAX(amount_aed) AS max_transaction_size_aed,
        
        -- Channel distribution
        COUNT(DISTINCT CASE WHEN is_international = TRUE THEN transaction_id END) AS international_txn_count,
        SUM(CASE WHEN is_international = TRUE THEN amount_aed ELSE 0 END) AS international_volume_aed,
        
        -- Merchant categories
        COUNT(DISTINCT merchant_category) AS distinct_merchant_categories,
        
        -- Time patterns
        COUNT(DISTINCT CASE WHEN time_of_day = 'OVERNIGHT' THEN transaction_id END) AS overnight_txn_count,
        COUNT(DISTINCT CASE WHEN day_type = 'WEEKEND' THEN transaction_id END) AS weekend_txn_count,
        
        -- Risk indicators
        COUNT(DISTINCT CASE WHEN high_value_flag = TRUE THEN transaction_id END) AS high_value_txn_count,
        SUM(CASE WHEN high_value_flag = TRUE THEN amount_aed ELSE 0 END) AS high_value_volume_aed,
        
        COUNT(DISTINCT CASE WHEN velocity_anomaly_flag = TRUE THEN transaction_id END) AS velocity_anomaly_count,
        COUNT(DISTINCT CASE WHEN high_risk_country_flag = TRUE THEN transaction_id END) AS high_risk_country_count,
        
        -- Status breakdown
        COUNT(DISTINCT CASE WHEN status = 'FAILED' THEN transaction_id END) AS failed_txn_count,
        COUNT(DISTINCT CASE WHEN status = 'COMPLETED' THEN transaction_id END) AS completed_txn_count
        
    FROM {{ ref('transactions_silver') }}
    
    {% if is_incremental() %}
        WHERE transaction_date > (SELECT MAX(analytics_date) FROM {{ this }})
    {% endif %}
    
    GROUP BY transaction_date
),

-- Add calculated metrics
enriched AS (
    SELECT
        *,
        
        -- Success rate
        CAST(completed_txn_count AS DOUBLE) / NULLIF(total_transactions, 0) * 100 AS success_rate_pct,
        
        -- International penetration
        CAST(international_txn_count AS DOUBLE) / NULLIF(total_transactions, 0) * 100 AS international_txn_pct,
        
        -- High-value concentration
        high_value_volume_aed / NULLIF(total_volume_aed, 0) * 100 AS high_value_volume_pct,
        
        -- Average transactions per active account
        CAST(total_transactions AS DOUBLE) / NULLIF(active_accounts, 0) AS avg_txn_per_account,
        
        -- Debit/Credit ratio
        total_debit_aed / NULLIF(total_credit_aed, 0) AS debit_credit_ratio,
        
        -- Risk score (0-100)
        LEAST(100, CAST(
            (velocity_anomaly_count / NULLIF(total_transactions, 0) * 1000) +
            (high_risk_country_count / NULLIF(total_transactions, 0) * 2000) +
            (failed_txn_count / NULLIF(total_transactions, 0) * 500)
        AS INT)) AS daily_risk_score,
        
        -- Day of week
        DAYOFWEEK(analytics_date) AS day_of_week,
        date_format(analytics_date, 'EEEE') AS day_name,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS gold_processed_at
        
    FROM daily_transactions
)

SELECT * FROM enriched
ORDER BY analytics_date DESC