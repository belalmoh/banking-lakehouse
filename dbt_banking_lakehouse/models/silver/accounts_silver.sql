{{
  config(
    materialized='incremental',
    unique_key='account_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by=['account_type'],
    post_hook=[
      "OPTIMIZE {{ this }} ZORDER BY (account_id, customer_id)"
    ]
  )
}}

/*
Silver Layer - Accounts
=======================
Clean, typed account data with balance categorization

Interview points:
- "Balance is cast to DECIMAL(18,2) for precision in financial calculations"
- "Account health status derived from balance and activity"
- "Dormant account flagging for compliance reporting"
*/

WITH source_data AS (
    SELECT
        -- Keys
        account_id,
        customer_id,
        
        -- Account attributes
        account_type,
        currency,
        CAST(balance AS DECIMAL(18, 2)) AS balance,
        status,
        
        -- Dates
        opened_date,
        last_activity_date,
        DATEDIFF(CURRENT_DATE(), last_activity_date) AS days_since_activity,
        DATEDIFF(CURRENT_DATE(), opened_date) AS account_age_days,
        
        -- Audit
        _ingestion_timestamp,
        _source_system,
        _data_classification,
        _data_residency,
        CURRENT_TIMESTAMP() AS silver_processed_at
        
    FROM {{ bronze_table('accounts') }}
    
    WHERE 1=1
        AND account_id IS NOT NULL
        AND customer_id IS NOT NULL
        AND currency IN ('AED', 'USD', 'EUR', 'GBP', 'SAR', 'INR')
        
    {% if is_incremental() %}
        AND _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

enriched_data AS (
    SELECT
        *,
        
        -- Balance bands
        CASE
            WHEN balance < 0 THEN 'NEGATIVE'
            WHEN balance = 0 THEN 'ZERO'
            WHEN balance BETWEEN 0.01 AND 10000 THEN 'LOW'
            WHEN balance BETWEEN 10000.01 AND 100000 THEN 'MEDIUM'
            WHEN balance BETWEEN 100000.01 AND 500000 THEN 'HIGH'
            ELSE 'VERY_HIGH'
        END AS balance_band,
        
        -- Account health status
        CASE
            WHEN status = 'CLOSED' THEN 'CLOSED'
            WHEN days_since_activity > 90 THEN 'DORMANT'
            WHEN days_since_activity > 30 THEN 'INACTIVE'
            ELSE 'ACTIVE'
        END AS account_health,
        
        -- Convert to AED for standardized reporting
        CASE currency
            WHEN 'USD' THEN balance * 3.67
            WHEN 'EUR' THEN balance * 4.02
            WHEN 'GBP' THEN balance * 4.65
            WHEN 'SAR' THEN balance * 0.98
            WHEN 'INR' THEN balance * 0.044
            ELSE balance  -- AED
        END AS balance_aed_equivalent,
        
        -- Compliance flags
        CASE
            WHEN account_type IN ('SAVINGS', 'CHECKING') AND balance > 1000000
                THEN TRUE
            ELSE FALSE
        END AS high_value_account_flag
        
    FROM source_data
)

SELECT * FROM enriched_data