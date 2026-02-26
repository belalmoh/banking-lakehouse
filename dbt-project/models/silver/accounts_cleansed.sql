-- =============================================================================
-- Silver Layer: Cleansed Accounts
-- Deduplication, status standardization, date casting
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='account_id',
        incremental_strategy='merge',
        tags=['silver', 'accounts']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'accounts') }}
    {% if is_incremental() %}
    WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY _ingestion_timestamp DESC
        ) AS _row_num
    FROM source
),

cleansed AS (
    SELECT
        account_id,
        customer_id,
        UPPER(TRIM(account_type)) AS account_type,
        UPPER(TRIM(currency)) AS currency,
        CAST(balance AS DECIMAL(18, 2)) AS balance,
        UPPER(TRIM(status)) AS status,
        CAST(opened_date AS DATE) AS opened_date,

        -- Derived: account age in days
        DATEDIFF(CURRENT_DATE(), CAST(opened_date AS DATE)) AS account_age_days,

        -- Audit columns
        _ingestion_timestamp,
        _source_file,
        _data_classification,
        _data_residency,
        CURRENT_TIMESTAMP() AS _cleansed_timestamp

    FROM deduplicated
    WHERE _row_num = 1
      AND account_id IS NOT NULL
      AND customer_id IS NOT NULL
)

SELECT * FROM cleansed
