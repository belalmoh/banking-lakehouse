-- =============================================================================
-- Gold Layer: CBUAE Regulatory Report
-- Monthly transaction summary with privacy-preserving customer identifiers
-- =============================================================================

{{
    config(
        materialized='table',
        tags=['gold', 'regulatory', 'cbuae']
    )
}}

WITH customers AS (
    SELECT
        customer_id,
        kyc_status,
        risk_score,
        risk_category,
        nationality
    FROM {{ ref('customers_cleansed') }}
),

accounts AS (
    SELECT account_id, customer_id
    FROM {{ ref('accounts_cleansed') }}
),

transactions AS (
    SELECT * FROM {{ ref('transactions_cleansed') }}
),

aml_alerts AS (
    SELECT account_id, alert_id
    FROM {{ ref('aml_alerts_cleansed') }}
),

-- Monthly transaction aggregation per customer
monthly_txn AS (
    SELECT
        a.customer_id,
        DATE_FORMAT(t.transaction_timestamp, 'yyyy-MM') AS report_month,
        COUNT(t.transaction_id) AS transaction_count,
        ROUND(SUM(t.amount), 2) AS total_amount,
        ROUND(SUM(CASE WHEN t.transaction_type = 'DEBIT' THEN t.amount ELSE 0 END), 2) AS total_debit,
        ROUND(SUM(CASE WHEN t.transaction_type = 'CREDIT' THEN t.amount ELSE 0 END), 2) AS total_credit,
        SUM(CASE WHEN t.is_international = TRUE THEN 1 ELSE 0 END) AS international_transaction_count,
        ROUND(SUM(CASE WHEN t.is_international = TRUE THEN t.amount ELSE 0 END), 2) AS international_amount,
        SUM(CASE WHEN t.aml_flag = TRUE THEN 1 ELSE 0 END) AS aml_flagged_count,
        {{ convert_currency('SUM(t.amount)', 't.currency', "'AED'") }} AS total_amount_aed
    FROM transactions t
    INNER JOIN accounts a ON t.account_id = a.account_id
    GROUP BY a.customer_id, DATE_FORMAT(t.transaction_timestamp, 'yyyy-MM')
),

-- AML alert count per customer
aml_count AS (
    SELECT
        a.customer_id,
        COUNT(al.alert_id) AS aml_alert_count
    FROM aml_alerts al
    INNER JOIN accounts a ON al.account_id = a.account_id
    GROUP BY a.customer_id
),

-- Build regulatory report
regulatory_report AS (
    SELECT
        -- Privacy-preserving customer identifier
        SHA2(c.customer_id, 256) AS customer_hash,

        mt.report_month,
        c.kyc_status,
        c.risk_score,
        c.risk_category,
        c.nationality,

        -- Transaction summary
        mt.transaction_count,
        mt.total_amount,
        mt.total_debit,
        mt.total_credit,
        mt.international_transaction_count,
        mt.international_amount,
        mt.aml_flagged_count,
        mt.total_amount_aed,

        -- AML exposure
        COALESCE(aml.aml_alert_count, 0) AS aml_alert_count,

        -- Regulatory metadata
        CURRENT_TIMESTAMP() AS report_generated_at,
        '{{ var("data_residency") }}' AS data_residency,
        'CBUAE' AS regulatory_body,
        'v1.0' AS report_version,

        -- Audit trail
        CURRENT_TIMESTAMP() AS _gold_timestamp

    FROM monthly_txn mt
    INNER JOIN customers c ON mt.customer_id = c.customer_id
    LEFT JOIN aml_count aml ON c.customer_id = aml.customer_id
)

SELECT * FROM regulatory_report
ORDER BY report_month DESC, risk_category DESC, total_amount DESC
