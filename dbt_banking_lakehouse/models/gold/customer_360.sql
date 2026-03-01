{{
  config(
    materialized='table',
    file_format='delta',
    location_root='s3a://gold',
    partition_by=['customer_segment'],
    post_hook=[
      "OPTIMIZE {{ this }} ZORDER BY (customer_id, total_balance_aed)"
    ],
    tags=['customer_360', 'daily']
  )
}}

/*
GOLD LAYER - CUSTOMER 360
=========================
Unified customer view combining demographics, holdings, behavior, and risk

Use Cases:
- Personalized banking offers (based on segment + balance)
- Customer retention analysis (tenure + activity)
- Cross-sell opportunities (product gaps)
- Risk-based pricing (risk score + transaction patterns)

Interview talking points:
- "Customer 360 is the foundation for ADCB's AI personalization"
- "Pre-aggregates all customer touchpoints for sub-second dashboard queries"
- "Denormalized design eliminates joins at query time"
- "Updated daily in batch, could move to streaming with Delta Live Tables"
*/

WITH customer_base AS (
    -- Core customer attributes from Silver
    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        phone_clean AS phone,
        date_of_birth,
        age_years,
        age_band,
        nationality,
        country_residence,
        customer_segment,
        risk_score,
        risk_category,
        risk_tier,
        kyc_status,
        is_pep,
        onboarding_date,
        customer_tenure_days,
        ROUND(customer_tenure_days / 365.0, 1) AS customer_tenure_years
    FROM {{ ref('customers_silver') }}
),

account_holdings AS (
    -- Aggregate all account balances by customer
    SELECT
        customer_id,
        COUNT(DISTINCT account_id) AS total_accounts,
        COUNT(DISTINCT CASE WHEN account_type = 'SAVINGS' THEN account_id END) AS savings_accounts,
        COUNT(DISTINCT CASE WHEN account_type = 'CHECKING' THEN account_id END) AS checking_accounts,
        COUNT(DISTINCT CASE WHEN account_type = 'CREDIT_CARD' THEN account_id END) AS credit_cards,
        COUNT(DISTINCT CASE WHEN account_type = 'PERSONAL_LOAN' THEN account_id END) AS personal_loans,
        COUNT(DISTINCT CASE WHEN account_type = 'MORTGAGE' THEN account_id END) AS mortgages,
        
        -- Total balances (converted to AED)
        SUM(balance_aed_equivalent) AS total_balance_aed,
        SUM(CASE WHEN balance_aed_equivalent > 0 THEN balance_aed_equivalent ELSE 0 END) AS total_assets_aed,
        SUM(CASE WHEN balance_aed_equivalent < 0 THEN ABS(balance_aed_equivalent) ELSE 0 END) AS total_liabilities_aed,
        
        -- Account health
        COUNT(DISTINCT CASE WHEN account_health = 'ACTIVE' THEN account_id END) AS active_accounts,
        COUNT(DISTINCT CASE WHEN account_health = 'DORMANT' THEN account_id END) AS dormant_accounts,
        
        -- High-value account flag
        MAX(CASE WHEN high_value_account_flag = TRUE THEN 1 ELSE 0 END) AS has_high_value_account,
        
        -- Currency diversification
        COUNT(DISTINCT currency) AS currency_diversity,
        
        -- Most recent account activity
        MAX(last_activity_date) AS last_account_activity
        
    FROM {{ ref('accounts_silver') }}
    GROUP BY customer_id
),

transaction_behavior AS (
    -- Last 90 days transaction patterns
    SELECT
        a.customer_id,
        
        -- Transaction volumes
        COUNT(DISTINCT t.transaction_id) AS txn_count_90d,
        COUNT(DISTINCT DATE(t.transaction_timestamp)) AS txn_active_days_90d,
        
        -- Transaction amounts
        SUM(t.amount_aed) AS total_txn_volume_90d,
        AVG(t.amount_aed) AS avg_txn_amount_90d,
        MAX(t.amount_aed) AS max_txn_amount_90d,
        
        -- Transaction patterns
        SUM(CASE WHEN t.is_international = TRUE THEN 1 ELSE 0 END) AS international_txn_count_90d,
        SUM(CASE WHEN t.high_value_flag = TRUE THEN 1 ELSE 0 END) AS high_value_txn_count_90d,
        
        -- Channel preferences (inferred from merchant categories)
        SUM(CASE WHEN t.merchant_category = 'Cash Withdrawal' THEN 1 ELSE 0 END) AS atm_usage_90d,
        SUM(CASE WHEN t.merchant_category IN ('Shopping', 'Electronics', 'Groceries') THEN 1 ELSE 0 END) AS retail_txn_90d,
        SUM(CASE WHEN t.merchant_category = 'Travel' THEN 1 ELSE 0 END) AS travel_txn_90d,
        
        -- Temporal patterns
        SUM(CASE WHEN t.time_of_day = 'OVERNIGHT' THEN 1 ELSE 0 END) AS overnight_txn_count_90d,
        SUM(CASE WHEN t.day_type = 'WEEKEND' THEN 1 ELSE 0 END) AS weekend_txn_count_90d,
        
        -- Most recent transaction
        MAX(t.transaction_timestamp) AS last_transaction_timestamp
        
    FROM {{ ref('accounts_silver') }} a
    LEFT JOIN {{ ref('transactions_silver') }} t
        ON a.account_id = t.account_id
        AND t.transaction_timestamp >= CURRENT_DATE() - INTERVAL 90 DAY
    GROUP BY a.customer_id
),

aml_profile AS (
    -- AML alert history
    SELECT
        customer_id,
        COUNT(DISTINCT alert_id) AS total_aml_alerts,
        COUNT(DISTINCT CASE WHEN severity = 'CRITICAL' THEN alert_id END) AS critical_alerts,
        COUNT(DISTINCT CASE WHEN severity = 'HIGH' THEN alert_id END) AS high_alerts,
        COUNT(DISTINCT CASE WHEN status = 'CLEARED' THEN alert_id END) AS cleared_alerts,
        COUNT(DISTINCT CASE WHEN status = 'ESCALATED' THEN alert_id END) AS escalated_alerts,
        MAX(created_at) AS last_alert_date,
        
        -- Alert types
        COUNT(DISTINCT CASE WHEN alert_type = 'HIGH_VALUE_TRANSACTION' THEN alert_id END) AS high_value_alerts,
        COUNT(DISTINCT CASE WHEN alert_type = 'SUSPICIOUS_PATTERN' THEN alert_id END) AS suspicious_pattern_alerts,
        COUNT(DISTINCT CASE WHEN alert_type = 'VELOCITY_ANOMALY' THEN alert_id END) AS velocity_alerts,
        
        -- AML risk indicator (0-100)
        CASE
            WHEN COUNT(DISTINCT CASE WHEN severity = 'CRITICAL' THEN alert_id END) > 0 THEN 90
            WHEN COUNT(DISTINCT CASE WHEN severity = 'HIGH' THEN alert_id END) >= 3 THEN 75
            WHEN COUNT(DISTINCT CASE WHEN severity = 'HIGH' THEN alert_id END) >= 1 THEN 60
            WHEN COUNT(DISTINCT CASE WHEN severity = 'MEDIUM' THEN alert_id END) >= 3 THEN 45
            WHEN COUNT(DISTINCT alert_id) > 0 THEN 30
            ELSE 10
        END AS aml_risk_indicator
        
    FROM {{ ref('aml_alerts_silver') }}
    GROUP BY customer_id
),

product_gaps AS (
    -- Identify cross-sell opportunities
    SELECT
        c.customer_id,
        
        -- Product ownership flags
        CASE WHEN h.savings_accounts > 0 THEN 1 ELSE 0 END AS has_savings,
        CASE WHEN h.checking_accounts > 0 THEN 1 ELSE 0 END AS has_checking,
        CASE WHEN h.credit_cards > 0 THEN 1 ELSE 0 END AS has_credit_card,
        CASE WHEN h.personal_loans > 0 THEN 1 ELSE 0 END AS has_personal_loan,
        CASE WHEN h.mortgages > 0 THEN 1 ELSE 0 END AS has_mortgage,
        
        -- Cross-sell opportunities (based on segment)
        CASE
            WHEN c.customer_segment = 'PRIVATE_BANKING' AND h.mortgages = 0 
                THEN 'MORTGAGE_UPSELL'
            WHEN c.customer_segment IN ('RETAIL', 'SME') AND h.credit_cards = 0 
                THEN 'CREDIT_CARD_OFFER'
            WHEN c.customer_segment = 'CORPORATE' AND h.checking_accounts < 2
                THEN 'MULTI_CURRENCY_ACCOUNT'
            WHEN c.age_years < 30 AND h.savings_accounts = 0
                THEN 'YOUTH_SAVINGS'
            ELSE NULL
        END AS recommended_product
        
    FROM customer_base c
    LEFT JOIN account_holdings h ON c.customer_id = h.customer_id
),

engagement_score AS (
    -- Calculate customer engagement (0-100)
    SELECT
        c.customer_id,
        
        -- Engagement components
        CASE WHEN tb.txn_count_90d > 0 THEN 25 ELSE 0 END AS engagement_recency,
        LEAST(tb.txn_count_90d / 30.0 * 25, 25) AS engagement_frequency,
        LEAST(tb.total_txn_volume_90d / 50000.0 * 25, 25) AS engagement_monetary,
        CASE WHEN h.total_accounts >= 3 THEN 25 ELSE h.total_accounts * 8 END AS engagement_product_holding,
        
        -- Total engagement score
        (
            CASE WHEN tb.txn_count_90d > 0 THEN 25 ELSE 0 END +
            LEAST(tb.txn_count_90d / 30.0 * 25, 25) +
            LEAST(tb.total_txn_volume_90d / 50000.0 * 25, 25) +
            CASE WHEN h.total_accounts >= 3 THEN 25 ELSE h.total_accounts * 8 END
        ) AS total_engagement_score,
        
        -- Engagement tier
        CASE
            WHEN (
                CASE WHEN tb.txn_count_90d > 0 THEN 25 ELSE 0 END +
                LEAST(tb.txn_count_90d / 30.0 * 25, 25) +
                LEAST(tb.total_txn_volume_90d / 50000.0 * 25, 25) +
                CASE WHEN h.total_accounts >= 3 THEN 25 ELSE h.total_accounts * 8 END
            ) >= 75 THEN 'HIGHLY_ENGAGED'
            WHEN (
                CASE WHEN tb.txn_count_90d > 0 THEN 25 ELSE 0 END +
                LEAST(tb.txn_count_90d / 30.0 * 25, 25) +
                LEAST(tb.total_txn_volume_90d / 50000.0 * 25, 25) +
                CASE WHEN h.total_accounts >= 3 THEN 25 ELSE h.total_accounts * 8 END
            ) >= 50 THEN 'MODERATELY_ENGAGED'
            WHEN (
                CASE WHEN tb.txn_count_90d > 0 THEN 25 ELSE 0 END +
                LEAST(tb.txn_count_90d / 30.0 * 25, 25) +
                LEAST(tb.total_txn_volume_90d / 50000.0 * 25, 25) +
                CASE WHEN h.total_accounts >= 3 THEN 25 ELSE h.total_accounts * 8 END
            ) >= 25 THEN 'LOW_ENGAGEMENT'
            ELSE 'INACTIVE'
        END AS engagement_tier
        
    FROM customer_base c
    LEFT JOIN transaction_behavior tb ON c.customer_id = tb.customer_id
    LEFT JOIN account_holdings h ON c.customer_id = h.customer_id
)

-- Final Customer 360 assembly
SELECT
    -- Customer identity
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    
    -- Demographics
    c.age_years,
    c.age_band,
    c.nationality,
    c.country_residence,
    c.customer_segment,
    c.customer_tenure_years,
    c.onboarding_date,
    
    -- Risk profile
    c.risk_score,
    c.risk_category,
    c.risk_tier,
    c.kyc_status,
    c.is_pep,
    COALESCE(aml.aml_risk_indicator, 10) AS aml_risk_indicator,
    
    -- Account holdings
    COALESCE(h.total_accounts, 0) AS total_accounts,
    COALESCE(h.savings_accounts, 0) AS savings_accounts,
    COALESCE(h.checking_accounts, 0) AS checking_accounts,
    COALESCE(h.credit_cards, 0) AS credit_cards,
    COALESCE(h.personal_loans, 0) AS personal_loans,
    COALESCE(h.mortgages, 0) AS mortgages,
    COALESCE(h.total_balance_aed, 0) AS total_balance_aed,
    COALESCE(h.total_assets_aed, 0) AS total_assets_aed,
    COALESCE(h.total_liabilities_aed, 0) AS total_liabilities_aed,
    COALESCE(h.active_accounts, 0) AS active_accounts,
    COALESCE(h.dormant_accounts, 0) AS dormant_accounts,
    COALESCE(h.currency_diversity, 0) AS currency_diversity,
    h.last_account_activity,
    
    -- Transaction behavior (90 days)
    COALESCE(tb.txn_count_90d, 0) AS txn_count_90d,
    COALESCE(tb.txn_active_days_90d, 0) AS txn_active_days_90d,
    COALESCE(tb.total_txn_volume_90d, 0) AS total_txn_volume_90d,
    COALESCE(tb.avg_txn_amount_90d, 0) AS avg_txn_amount_90d,
    COALESCE(tb.international_txn_count_90d, 0) AS international_txn_count_90d,
    COALESCE(tb.high_value_txn_count_90d, 0) AS high_value_txn_count_90d,
    COALESCE(tb.atm_usage_90d, 0) AS atm_usage_90d,
    COALESCE(tb.retail_txn_90d, 0) AS retail_txn_90d,
    COALESCE(tb.travel_txn_90d, 0) AS travel_txn_90d,
    tb.last_transaction_timestamp,
    
    -- AML profile
    COALESCE(aml.total_aml_alerts, 0) AS total_aml_alerts,
    COALESCE(aml.critical_alerts, 0) AS critical_alerts,
    COALESCE(aml.cleared_alerts, 0) AS cleared_alerts,
    COALESCE(aml.escalated_alerts, 0) AS escalated_alerts,
    aml.last_alert_date,
    
    -- Cross-sell opportunities
    pg.has_savings,
    pg.has_checking,
    pg.has_credit_card,
    pg.has_personal_loan,
    pg.has_mortgage,
    pg.recommended_product,
    
    -- Engagement metrics
    es.total_engagement_score,
    es.engagement_tier,
    
    -- Customer lifetime value (simplified)
    ROUND(
        COALESCE(h.total_balance_aed, 0) * 0.02 +  -- 2% NIM on deposits
        COALESCE(tb.total_txn_volume_90d, 0) * 4 * 0.003  -- Transaction fees annualized
    , 2) AS estimated_annual_revenue_aed,
    
    -- Churn risk indicator (simplified)
    CASE
        WHEN DATEDIFF(CURRENT_DATE(), COALESCE(tb.last_transaction_timestamp, h.last_account_activity)) > 90
             AND es.total_engagement_score < 25
        THEN 'HIGH_CHURN_RISK'
        WHEN DATEDIFF(CURRENT_DATE(), COALESCE(tb.last_transaction_timestamp, h.last_account_activity)) > 60
             AND es.total_engagement_score < 50
        THEN 'MEDIUM_CHURN_RISK'
        ELSE 'LOW_CHURN_RISK'
    END AS churn_risk,
    
    -- Data lineage
    CURRENT_TIMESTAMP() AS gold_updated_at

FROM customer_base c
LEFT JOIN account_holdings h ON c.customer_id = h.customer_id
LEFT JOIN transaction_behavior tb ON c.customer_id = tb.customer_id
LEFT JOIN aml_profile aml ON c.customer_id = aml.customer_id
LEFT JOIN product_gaps pg ON c.customer_id = pg.customer_id
LEFT JOIN engagement_score es ON c.customer_id = es.customer_id