-- =============================================================================
-- Gold Layer: AML Summary
-- Aggregated AML intelligence by customer and alert type
-- =============================================================================

{{
    config(
        materialized='table',
        tags=['gold', 'aml', 'compliance']
    )
}}

WITH alerts AS (
    SELECT * FROM {{ ref('aml_alerts_cleansed') }}
),

accounts AS (
    SELECT account_id, customer_id, account_type
    FROM {{ ref('accounts_cleansed') }}
),

transactions AS (
    SELECT transaction_id, amount, currency, is_international, country
    FROM {{ ref('transactions_cleansed') }}
),

-- Enrich alerts with transaction and account context
enriched_alerts AS (
    SELECT
        al.alert_id,
        al.alert_type,
        al.severity,
        al.severity_score,
        al.status,
        al.created_at,
        al.assigned_to,
        al.alert_age_hours,
        acc.customer_id,
        acc.account_type,
        t.amount,
        t.currency,
        t.is_international,
        t.country AS transaction_country
    FROM alerts al
    LEFT JOIN accounts acc ON al.account_id = acc.account_id
    LEFT JOIN transactions t ON al.transaction_id = t.transaction_id
),

-- Aggregate by customer and alert type
aml_summary AS (
    SELECT
        customer_id,
        alert_type,
        severity,

        -- Alert counts
        COUNT(alert_id) AS alert_count,
        SUM(CASE WHEN status = 'NEW' THEN 1 ELSE 0 END) AS new_count,
        SUM(CASE WHEN status = 'INVESTIGATING' THEN 1 ELSE 0 END) AS investigating_count,
        SUM(CASE WHEN status = 'CLEARED' THEN 1 ELSE 0 END) AS cleared_count,
        SUM(CASE WHEN status = 'ESCALATED' THEN 1 ELSE 0 END) AS escalated_count,

        -- Financial impact
        ROUND(SUM(amount), 2) AS total_flagged_amount,
        ROUND(AVG(amount), 2) AS avg_flagged_amount,
        MAX(amount) AS max_flagged_amount,

        -- International exposure
        SUM(CASE WHEN is_international = TRUE THEN 1 ELSE 0 END) AS international_alert_count,

        -- Timeline
        MIN(created_at) AS first_alert_date,
        MAX(created_at) AS last_alert_date,
        ROUND(AVG(alert_age_hours), 1) AS avg_alert_age_hours,

        -- Resolution rate
        ROUND(
            SUM(CASE WHEN status = 'CLEARED' THEN 1 ELSE 0 END) * 100.0 / COUNT(alert_id),
            1
        ) AS resolution_rate_pct,

        -- Metadata
        CURRENT_TIMESTAMP() AS _gold_timestamp

    FROM enriched_alerts
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id, alert_type, severity
)

SELECT * FROM aml_summary
