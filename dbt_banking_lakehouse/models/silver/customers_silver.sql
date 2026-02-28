{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by=['customer_segment'],
    merge_update_columns=['email', 'phone', 'kyc_status', 'risk_score', 'risk_category'],
    post_hook=[
      "OPTIMIZE {{ this }} ZORDER BY (customer_id, risk_score)"
    ]
  )
}}

/*
Silver Layer - Customers
========================
Transforms raw customer data into clean, business-ready format

Transformations:
- Type casting and normalization
- Email validation and standardization
- Age calculation from date_of_birth
- Risk category derivation
- PII masking preparation (for Gold layer)
- Deduplication based on customer_id

Interview talking points:
- "This incremental model uses MERGE strategy for efficient updates"
- "SCD Type 2 tracking is handled via dbt snapshots (separate model)"
- "Z-ordering on customer_id and risk_score optimizes AML queries"
- "Business rules validated: age >= 18, valid email format"
*/

WITH source_data AS (
    SELECT
        -- Business keys
        customer_id,
        
        -- Personal information (cleaned)
        TRIM(UPPER(first_name)) AS first_name,
        TRIM(UPPER(last_name)) AS last_name,
        LOWER(TRIM(email)) AS email,
        REGEXP_REPLACE(phone, '[^0-9+\\-]', '') AS phone_clean,
        
        -- Demographics
        date_of_birth,
        DATEDIFF(CURRENT_DATE(), date_of_birth) / 365 AS age_years,
        UPPER(nationality) AS nationality,
        UPPER(country_residence) AS country_residence,
        
        -- Customer classification
        customer_segment,
        risk_score,
        risk_category,
        
        -- KYC/Compliance
        kyc_status,
        is_pep,
        onboarding_date,
        
        -- Audit trail
        _ingestion_timestamp,
        _source_system,
        _data_classification,
        _data_residency,
        _record_hash,
        CURRENT_TIMESTAMP() AS silver_processed_at
        
    FROM {{ bronze_table('customers') }}
    
    WHERE 1=1
        -- Data quality filters
        AND customer_id IS NOT NULL
        AND first_name IS NOT NULL
        AND last_name IS NOT NULL
        AND email IS NOT NULL
        AND email LIKE '%@%'  -- Basic email validation
        AND DATEDIFF(CURRENT_DATE(), date_of_birth) / 365 >= 18  -- Age >= 18
        AND risk_score BETWEEN 1 AND 100
        
    {% if is_incremental() %}
        -- Incremental filter: only new or updated records
        AND _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

-- Add derived business attributes
enriched_data AS (
    SELECT
        *,
        
        -- Age band for segmentation
        CASE
            WHEN age_years < 25 THEN 'YOUNG'
            WHEN age_years BETWEEN 25 AND 40 THEN 'MIDDLE_AGED'
            WHEN age_years BETWEEN 41 AND 60 THEN 'MATURE'
            ELSE 'SENIOR'
        END AS age_band,
        
        -- Email domain (for analytics)
        SPLIT(email, '@')[1] AS email_domain,
        
        -- Risk tier (simplified)
        CASE
            WHEN risk_score <= 30 THEN 'LOW_RISK'
            WHEN risk_score <= 60 THEN 'MEDIUM_RISK'
            WHEN risk_score <= 85 THEN 'HIGH_RISK'
            ELSE 'CRITICAL_RISK'
        END AS risk_tier,
        
        -- Account tenure
        DATEDIFF(CURRENT_DATE(), onboarding_date) AS customer_tenure_days,
        
        -- Data quality score (0-100)
        CASE
            WHEN phone_clean IS NOT NULL AND LENGTH(phone_clean) >= 10 THEN 100
            WHEN phone_clean IS NULL THEN 70
            ELSE 85
        END AS data_quality_score
        
    FROM source_data
),

-- Deduplication (keep latest version based on _ingestion_timestamp)
deduplicated AS (
    SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id 
                ORDER BY _ingestion_timestamp DESC
            ) AS row_num
        FROM enriched_data
    )
    WHERE row_num = 1
)

SELECT
    customer_id, first_name, last_name, email, phone_clean,
    date_of_birth, age_years, nationality, country_residence,
    customer_segment, risk_score, risk_category,
    kyc_status, is_pep, onboarding_date,
    _ingestion_timestamp, _source_system, _data_classification,
    _data_residency, _record_hash, silver_processed_at,
    age_band, email_domain, risk_tier, customer_tenure_days, data_quality_score
FROM deduplicated