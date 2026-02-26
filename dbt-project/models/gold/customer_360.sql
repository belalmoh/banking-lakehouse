-- =============================================================================
-- Gold Layer: Customer 360 View
-- Comprehensive customer profile joining accounts, transactions, and AML alerts
-- =============================================================================

{{
    config(
        materialized='table',
        tags=['gold', 'customer_360']
    )
}}

WITH customers AS (
    SELECT * FROM {{ ref('customers_cleansed') }}
),

-- Aggregate accounts per customer
accounts_agg AS (
    SELECT
        customer_id,
        COUNT(DISTINCT account_id) AS total_accounts,
        SUM(balance) AS total_balance,
        COLLECT_SET(account_type) AS account_types,
        SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_accounts,
        MIN(opened_date) AS earliest_account_date
    FROM {{ ref('accounts_cleansed') }}
    GROUP BY customer_id
),

-- Aggregate transactions per customer (via accounts)
transactions_agg AS (
    SELECT
        a.customer_id,
        COUNT(t.transaction_id) AS total_transactions,
        SUM(CASE WHEN t.transaction_type = 'DEBIT' THEN t.amount ELSE 0 END) AS total_debit,
        SUM(CASE WHEN t.transaction_type = 'CREDIT' THEN t.amount ELSE 0 END) AS total_credit,
        ROUND(AVG(t.amount), 2) AS avg_transaction_amount,
        MAX(t.amount) AS max_transaction_amount,
        MIN(t.transaction_date) AS first_transaction_date,
        MAX(t.transaction_date) AS last_transaction_date,
        SUM(CASE WHEN t.is_international = TRUE THEN 1 ELSE 0 END) AS international_transactions,
        SUM(CASE WHEN t.aml_flag = TRUE THEN 1 ELSE 0 END) AS aml_flagged_transactions
    FROM {{ ref('transactions_cleansed') }} t
    INNER JOIN {{ ref('accounts_cleansed') }} a ON t.account_id = a.account_id
    GROUP BY a.customer_id
),

-- Aggregate AML alerts per customer
aml_agg AS (
    SELECT
        a.customer_id,
        COUNT(al.alert_id) AS aml_alert_count,
        MAX(al.severity) AS latest_aml_severity,
        MAX(al.severity_score) AS max_severity_score,
        SUM(CASE WHEN al.status = 'ESCALATED' THEN 1 ELSE 0 END) AS escalated_alerts,
        SUM(CASE WHEN al.status = 'NEW' THEN 1 ELSE 0 END) AS pending_alerts
    FROM {{ ref('aml_alerts_cleansed') }} al
    INNER JOIN {{ ref('accounts_cleansed') }} a ON al.account_id = a.account_id
    GROUP BY a.customer_id
),

-- Build Customer 360
customer_360 AS (
    SELECT
        c.customer_id,
        CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
        c.customer_segment,
        c.kyc_status,
        c.risk_score,
        c.risk_category,
        c.nationality,
        c.country,
        c.onboarding_date,
        c.email_valid,

        -- Account metrics
        COALESCE(acc.total_accounts, 0) AS total_accounts,
        COALESCE(acc.total_balance, 0) AS total_balance,
        acc.account_types,
        COALESCE(acc.active_accounts, 0) AS active_accounts,
        acc.earliest_account_date,

        -- Transaction metrics
        COALESCE(txn.total_transactions, 0) AS total_transactions,
        COALESCE(txn.total_debit, 0) AS total_debit,
        COALESCE(txn.total_credit, 0) AS total_credit,
        COALESCE(txn.avg_transaction_amount, 0) AS avg_transaction_amount,
        COALESCE(txn.max_transaction_amount, 0) AS max_transaction_amount,
        txn.first_transaction_date,
        txn.last_transaction_date,
        COALESCE(txn.international_transactions, 0) AS international_transactions,
        COALESCE(txn.aml_flagged_transactions, 0) AS aml_flagged_transactions,

        -- AML metrics
        COALESCE(aml.aml_alert_count, 0) AS aml_alert_count,
        aml.latest_aml_severity,
        COALESCE(aml.max_severity_score, 0) AS max_severity_score,
        COALESCE(aml.escalated_alerts, 0) AS escalated_alerts,
        COALESCE(aml.pending_alerts, 0) AS pending_alerts,

        -- Composite risk indicator
        {{ calculate_risk_score(
            'c.risk_score',
            'COALESCE(aml.aml_alert_count, 0)',
            'COALESCE(txn.aml_flagged_transactions, 0)'
        ) }} AS composite_risk_score,

        -- Metadata
        CURRENT_TIMESTAMP() AS _gold_timestamp

    FROM customers c
    LEFT JOIN accounts_agg acc ON c.customer_id = acc.customer_id
    LEFT JOIN transactions_agg txn ON c.customer_id = txn.customer_id
    LEFT JOIN aml_agg aml ON c.customer_id = aml.customer_id
)

SELECT * FROM customer_360
