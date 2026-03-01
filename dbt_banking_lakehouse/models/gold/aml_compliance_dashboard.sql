{{
  config(
    materialized='table',
    file_format='delta',
    location_root='s3a://gold',
    partition_by=['alert_severity'],
    post_hook=[
      "OPTIMIZE {{ this }} ZORDER BY (created_date, priority_rank)"
    ],
    tags=['aml', 'compliance', 'hourly']
  )
}}

/*
GOLD LAYER - AML COMPLIANCE DASHBOARD
======================================
Real-time view for compliance officers monitoring AML alerts

Use Cases:
- Compliance dashboard (Tableau/Power BI)
- SLA monitoring and breach alerting
- Analyst workload balancing
- CBUAE regulatory reporting

Interview talking points:
- "Pre-aggregates all context for instant alert triage"
- "SLA tracking ensures CBUAE compliance deadlines met"
- "Priority ranking optimizes analyst assignment"
- "Could be refreshed hourly or moved to streaming"
*/

WITH alert_details AS (
    SELECT
        a.alert_id,
        a.alert_type,
        a.severity AS alert_severity,
        a.status AS alert_status,
        a.assigned_to AS analyst_assigned,
        
        -- Timestamps
        a.created_at,
        DATE(a.created_at) AS created_date,
        a.last_updated,
        a.alert_age_hours,
        a.alert_age_days,
        
        -- SLA tracking
        a.sla_hours,
        a.sla_breach_flag,
        CASE
            WHEN a.sla_breach_flag = TRUE 
            THEN a.alert_age_hours - a.sla_hours
            ELSE 0
        END AS sla_breach_hours,
        
        a.investigation_duration_hours,
        a.priority_score,
        a.resolution_notes,
        
        -- Transaction context
        a.transaction_id,
        a.transaction_amount,
        a.transaction_country,
        
        -- Customer context
        a.customer_id,
        a.account_id,
        a.customer_risk_score
        
    FROM {{ ref('aml_alerts_silver') }} a
),

enriched_alerts AS (
    SELECT
        a.*,
        
        -- Customer details
        c.first_name || ' ' || c.last_name AS customer_name,
        c.customer_segment,
        c.kyc_status,
        c.is_pep,
        c.nationality,
        
        -- Transaction details
        t.merchant_name,
        t.merchant_category,
        t.is_international,
        t.high_value_flag,
        t.velocity_anomaly_flag,
        
        -- Account details
        ac.account_type,
        ac.balance_aed_equivalent AS account_balance_aed,
        ac.account_health,
        
        -- Priority ranking (for analyst queue)
        ROW_NUMBER() OVER (
            ORDER BY 
                CASE a.alert_severity
                    WHEN 'CRITICAL' THEN 1
                    WHEN 'HIGH' THEN 2
                    WHEN 'MEDIUM' THEN 3
                    ELSE 4
                END,
                CASE a.alert_status
                    WHEN 'NEW' THEN 1
                    WHEN 'INVESTIGATING' THEN 2
                    ELSE 3
                END,
                a.alert_age_hours DESC
        ) AS priority_rank,
        
        -- Investigation metrics
        CASE
            WHEN a.alert_status IN ('CLEARED', 'CLOSED') 
            THEN 'COMPLETED'
            WHEN a.sla_breach_flag = TRUE 
            THEN 'OVERDUE'
            WHEN a.alert_age_hours > (a.sla_hours * 0.8)
            THEN 'AT_RISK'
            ELSE 'ON_TRACK'
        END AS sla_status,
        
        -- Risk aggregation
        (a.customer_risk_score + 
         CASE a.alert_severity
            WHEN 'CRITICAL' THEN 40
            WHEN 'HIGH' THEN 30
            WHEN 'MEDIUM' THEN 20
            ELSE 10
         END +
         CASE WHEN c.is_pep = TRUE THEN 20 ELSE 0 END
        ) AS composite_risk_score
        
    FROM alert_details a
    LEFT JOIN {{ ref('customers_silver') }} c 
        ON a.customer_id = c.customer_id
    LEFT JOIN {{ ref('transactions_silver') }} t
        ON a.transaction_id = t.transaction_id
    LEFT JOIN {{ ref('accounts_silver') }} ac
        ON a.account_id = ac.account_id
)

SELECT
    -- Alert identification
    alert_id,
    alert_type,
    alert_severity,
    alert_status,
    analyst_assigned,
    
    -- Temporal
    created_at,
    created_date,
    last_updated,
    alert_age_hours,
    alert_age_days,
    
    -- SLA management
    sla_hours,
    sla_breach_flag,
    sla_breach_hours,
    sla_status,
    investigation_duration_hours,
    
    -- Priority & assignment
    priority_score,
    priority_rank,
    composite_risk_score,
    
    -- Customer context
    customer_id,
    customer_name,
    customer_segment,
    kyc_status,
    is_pep,
    nationality,
    customer_risk_score,
    
    -- Transaction context
    transaction_id,
    transaction_amount,
    transaction_country,
    merchant_name,
    merchant_category,
    is_international,
    high_value_flag,
    velocity_anomaly_flag,
    
    -- Account context
    account_id,
    account_type,
    account_balance_aed,
    account_health,
    
    -- Resolution
    resolution_notes,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS dashboard_refreshed_at

FROM enriched_alerts