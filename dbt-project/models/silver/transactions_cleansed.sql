-- =============================================================================
-- Silver Layer: Cleansed Transactions
-- Incremental materialization with deduplication, type casting, and AML flags
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        tags=['silver', 'transactions']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'transactions') }}
    {% if is_incremental() %}
    WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

-- Deduplicate: keep latest record per transaction_id
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY _ingestion_timestamp DESC
        ) AS _row_num
    FROM source
),

cleansed AS (
    SELECT
        transaction_id,
        account_id,
        UPPER(TRIM(transaction_type)) AS transaction_type,
        CAST(amount AS DECIMAL(18, 2)) AS amount,
        UPPER(TRIM(currency)) AS currency,
        TRIM(merchant_name) AS merchant_name,
        UPPER(TRIM(merchant_category)) AS merchant_category,
        CAST(transaction_timestamp AS TIMESTAMP) AS transaction_timestamp,
        CAST(transaction_timestamp AS DATE) AS transaction_date,
        HOUR(CAST(transaction_timestamp AS TIMESTAMP)) AS transaction_hour,
        UPPER(TRIM(country)) AS country,
        UPPER(TRIM(status)) AS status,
        CAST(is_international AS BOOLEAN) AS is_international,

        -- AML flagging
        CASE
            WHEN amount > {{ var('aml_high_value_threshold') }}
                AND UPPER(TRIM(currency)) = 'AED'
                THEN TRUE
            WHEN CAST(is_international AS BOOLEAN) = TRUE
                AND UPPER(TRIM(country)) IN ({{ "'" ~ var('high_risk_countries') | join("','") ~ "'" }})
                THEN TRUE
            ELSE FALSE
        END AS aml_flag,

        -- Audit columns
        _ingestion_timestamp,
        _source_file,
        _batch_id,
        _data_classification,
        _data_residency,
        CURRENT_TIMESTAMP() AS _cleansed_timestamp

    FROM deduplicated
    WHERE _row_num = 1
      AND amount > 0                                   -- Filter invalid amounts
      AND transaction_id IS NOT NULL                    -- Filter null keys
      AND account_id IS NOT NULL
      AND UPPER(TRIM(currency)) IN ({{ "'" ~ var('valid_currencies') | join("','") ~ "'" }})
)

SELECT * FROM cleansed
