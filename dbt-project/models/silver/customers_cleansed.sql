-- =============================================================================
-- Silver Layer: Cleansed Customers
-- Deduplication, name standardization, email validation, date casting
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge',
        tags=['silver', 'customers']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'customers') }}
    {% if is_incremental() %}
    WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _ingestion_timestamp DESC
        ) AS _row_num
    FROM source
),

cleansed AS (
    SELECT
        customer_id,
        INITCAP(TRIM(first_name)) AS first_name,
        INITCAP(TRIM(last_name)) AS last_name,
        LOWER(TRIM(email)) AS email,
        TRIM(phone) AS phone,
        CAST(date_of_birth AS DATE) AS date_of_birth,
        UPPER(TRIM(nationality)) AS nationality,
        CAST(risk_score AS INT) AS risk_score,
        UPPER(TRIM(kyc_status)) AS kyc_status,
        CAST(onboarding_date AS DATE) AS onboarding_date,
        UPPER(TRIM(customer_segment)) AS customer_segment,
        UPPER(TRIM(country)) AS country,

        -- Email validation flag
        CASE
            WHEN email RLIKE '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$'
            THEN TRUE
            ELSE FALSE
        END AS email_valid,

        -- Risk categorization
        CASE
            WHEN risk_score > 80 THEN 'HIGH'
            WHEN risk_score > 40 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_category,

        -- Audit columns
        _ingestion_timestamp,
        _source_file,
        _data_classification,
        _data_residency,
        CURRENT_TIMESTAMP() AS _cleansed_timestamp

    FROM deduplicated
    WHERE _row_num = 1
      AND customer_id IS NOT NULL
      AND first_name IS NOT NULL
      AND last_name IS NOT NULL
)

SELECT * FROM cleansed
