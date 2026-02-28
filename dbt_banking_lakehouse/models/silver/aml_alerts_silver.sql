{{
  config(
    materialized='incremental',
    unique_key='alert_id',
    file_format='delta',
    incremental_strategy='merge',
    partition_by=['alert_date'],
    merge_update_columns=['status', 'assigned_to', 'last_updated', 'resolution_notes']
  )
}}

/*
Silver Layer - AML Alerts
==========================
Regulatory compliance data with SLA tracking

Interview points:
- "Status updates handled via merge strategy (not append)"
- "SLA tracking based on severity (Critical: 4h, High: 24h, Medium: 72h)"
- "Alert aging calculated for compliance reporting"
*/

WITH source_data AS (
    SELECT
        alert_id,
        transaction_id,
        account_id,
        customer_id,
        alert_type,
        severity,
        status,
        assigned_to,
        
        -- Timestamps
        created_at,
        last_updated,
        DATE(created_at) AS alert_date,
        
        -- Additional context
        resolution_notes,
        customer_risk_score,
        CAST(transaction_amount AS DECIMAL(18, 2)) AS transaction_amount,
        transaction_country,
        
        -- Audit
        _ingestion_timestamp,
        _source_system,
        _data_classification,
        CURRENT_TIMESTAMP() AS silver_processed_at
        
    FROM {{ bronze_table('aml_alerts') }}
    
    WHERE alert_id IS NOT NULL
    
    {% if is_incremental() %}
        AND last_updated > (SELECT MAX(last_updated) FROM {{ this }})
    {% endif %}
),

enriched_data AS (
    SELECT
        *,
        
        -- Alert age tracking
        DATEDIFF(CURRENT_TIMESTAMP(), created_at) AS alert_age_days,
        TIMESTAMPDIFF(HOUR, created_at, CURRENT_TIMESTAMP()) AS alert_age_hours,
        
        -- SLA thresholds (hours)
        CASE severity
            WHEN 'CRITICAL' THEN 4
            WHEN 'HIGH' THEN 24
            WHEN 'MEDIUM' THEN 72
            ELSE 168  -- 7 days for LOW
        END AS sla_hours,
        
        -- SLA breach detection
        CASE
            WHEN status IN ('CLEARED', 'CLOSED') THEN FALSE
            WHEN severity = 'CRITICAL' 
                 AND TIMESTAMPDIFF(HOUR, created_at, CURRENT_TIMESTAMP()) > 4
            THEN TRUE
            WHEN severity = 'HIGH'
                 AND TIMESTAMPDIFF(HOUR, created_at, CURRENT_TIMESTAMP()) > 24
            THEN TRUE
            WHEN severity = 'MEDIUM'
                 AND TIMESTAMPDIFF(HOUR, created_at, CURRENT_TIMESTAMP()) > 72
            THEN TRUE
            ELSE FALSE
        END AS sla_breach_flag,
        
        -- Investigation duration (for closed alerts)
        CASE
            WHEN status IN ('CLEARED', 'CLOSED')
            THEN TIMESTAMPDIFF(HOUR, created_at, last_updated)
            ELSE NULL
        END AS investigation_duration_hours,
        
        -- Priority score (for analyst assignment)
        CASE severity
            WHEN 'CRITICAL' THEN 4
            WHEN 'HIGH' THEN 3
            WHEN 'MEDIUM' THEN 2
            ELSE 1
        END * 
        CASE status
            WHEN 'NEW' THEN 2
            WHEN 'INVESTIGATING' THEN 1.5
            ELSE 1
        END AS priority_score
        
    FROM source_data
)

SELECT * FROM enriched_data