{{
  config(
    materialized='incremental',
    unique_key='transaction_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by=['transaction_date'],
    post_hook=[
      "OPTIMIZE {{ this }} ZORDER BY (account_id, transaction_timestamp, amount)"
    ]
  )
}}

/*
Silver Layer - Transactions
============================
High-volume transaction processing with AML screening flags

Interview points:
- "Partitioned by transaction_date for efficient date-range queries"
- "Z-ordered on account_id and amount for AML screening performance"
- "Incremental strategy processes only new transactions (reduces runtime from hours to minutes)"
- "AML flags pre-computed for real-time screening dashboard"
*/

WITH source_data AS (
    SELECT
        -- Transaction identifiers
        transaction_id,
        account_id,
        
        -- Transaction details
        transaction_type,
        CAST(amount AS DECIMAL(18, 2)) AS amount,
        currency,
        merchant_name,
        merchant_category,
        
        -- Temporal
        transaction_timestamp,
        DATE(transaction_timestamp) AS transaction_date,
        HOUR(transaction_timestamp) AS transaction_hour,
        DAYOFWEEK(transaction_timestamp) AS transaction_day_of_week,
        
        -- Geography
        UPPER(country) AS country,
        is_international,
        
        -- Status
        status,
        description,
        
        -- Audit
        _ingestion_timestamp,
        _source_system,
        _data_classification,
        _data_residency,
        CURRENT_TIMESTAMP() AS silver_processed_at
        
    FROM {{ bronze_table('transactions') }}
    
    WHERE 1=1
        AND transaction_id IS NOT NULL
        AND account_id IS NOT NULL
        AND amount >= 0  -- Negative amounts indicate data quality issues
        AND status = 'COMPLETED'  -- Only process completed transactions
        AND transaction_timestamp <= CURRENT_TIMESTAMP()  -- No future transactions
        
    {% if is_incremental() %}
        AND transaction_timestamp > (
            SELECT MAX(transaction_timestamp) 
            FROM {{ this }}
        )
    {% endif %}
),

enriched_data AS (
    SELECT
        *,
        
        -- Convert to AED for standardized analysis
        CASE currency
            WHEN 'USD' THEN amount * 3.67
            WHEN 'EUR' THEN amount * 4.02
            WHEN 'GBP' THEN amount * 4.65
            WHEN 'SAR' THEN amount * 0.98
            WHEN 'INR' THEN amount * 0.044
            ELSE amount
        END AS amount_aed,
        
        -- Transaction categorization
        CASE
            WHEN amount < 100 THEN 'MICRO'
            WHEN amount BETWEEN 100 AND 1000 THEN 'SMALL'
            WHEN amount BETWEEN 1000 AND 10000 THEN 'MEDIUM'
            WHEN amount BETWEEN 10000 AND 50000 THEN 'LARGE'
            ELSE 'VERY_LARGE'
        END AS transaction_size,
        
        -- AML screening flags (pre-computed for performance)
        CASE
            WHEN amount > {{ var('high_value_threshold') }} THEN TRUE
            ELSE FALSE
        END AS high_value_flag,
        
        CASE
            WHEN is_international = TRUE 
                 AND country IN ('IRAN', 'SYRIA', 'NORTH_KOREA') 
            THEN TRUE
            ELSE FALSE
        END AS high_risk_country_flag,
        
        CASE
            WHEN amount % 1000 = 0 AND amount >= 10000 THEN TRUE
            ELSE FALSE
        END AS round_amount_flag,  -- Potential structuring
        
        -- Time-based patterns
        CASE
            WHEN transaction_hour BETWEEN 22 AND 5 THEN 'OVERNIGHT'
            WHEN transaction_hour BETWEEN 6 AND 11 THEN 'MORNING'
            WHEN transaction_hour BETWEEN 12 AND 17 THEN 'AFTERNOON'
            ELSE 'EVENING'
        END AS time_of_day,
        
        CASE
            WHEN transaction_day_of_week IN (1, 7) THEN 'WEEKEND'
            ELSE 'WEEKDAY'
        END AS day_type
        
    FROM source_data
),

-- Add velocity features (requires window functions)
with_velocity AS (
    SELECT
        *,
        
        -- Count transactions in last hour (for velocity detection)
        COUNT(*) OVER (
            PARTITION BY account_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
        ) AS txn_count_last_hour,
        
        -- Sum amount in last 24 hours
        SUM(amount_aed) OVER (
            PARTITION BY account_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL 24 HOUR PRECEDING AND CURRENT ROW
        ) AS total_amount_last_24h,
        
        -- Velocity anomaly flag
        CASE
            WHEN COUNT(*) OVER (
                PARTITION BY account_id
                ORDER BY transaction_timestamp
                RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW
            ) > 10
            THEN TRUE
            ELSE FALSE
        END AS velocity_anomaly_flag
        
    FROM enriched_data
)

SELECT * FROM with_velocity