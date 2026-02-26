-- =============================================================================
-- Silver Layer: Cleansed AML Alerts
-- Deduplication, severity/status standardization, timestamp casting
-- =============================================================================

{{
    config(
        materialized='incremental',
        unique_key='alert_id',
        incremental_strategy='merge',
        tags=['silver', 'aml']
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'aml_alerts') }}
    {% if is_incremental() %}
    WHERE _ingestion_timestamp > (SELECT MAX(_ingestion_timestamp) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY alert_id
            ORDER BY _ingestion_timestamp DESC
        ) AS _row_num
    FROM source
),

cleansed AS (
    SELECT
        alert_id,
        transaction_id,
        account_id,
        UPPER(TRIM(alert_type)) AS alert_type,
        UPPER(TRIM(severity)) AS severity,
        UPPER(TRIM(status)) AS status,
        CAST(created_at AS TIMESTAMP) AS created_at,
        TRIM(assigned_to) AS assigned_to,

        -- Severity numeric score for aggregation
        CASE UPPER(TRIM(severity))
            WHEN 'CRITICAL' THEN 4
            WHEN 'HIGH' THEN 3
            WHEN 'MEDIUM' THEN 2
            WHEN 'LOW' THEN 1
            ELSE 0
        END AS severity_score,

        -- Alert age in hours
        ROUND(
            (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(CAST(created_at AS TIMESTAMP))) / 3600,
            1
        ) AS alert_age_hours,

        -- Audit columns
        _ingestion_timestamp,
        _source_file,
        _data_classification,
        _data_residency,
        CURRENT_TIMESTAMP() AS _cleansed_timestamp

    FROM deduplicated
    WHERE _row_num = 1
      AND alert_id IS NOT NULL
)

SELECT * FROM cleansed
