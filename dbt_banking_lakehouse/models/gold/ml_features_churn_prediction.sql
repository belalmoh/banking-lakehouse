{{
  config(
    materialized='table',
    file_format='delta',
    location_root='s3a://gold',
    tags=['ml', 'feature_store', 'churn']
  )
}}

/*
GOLD LAYER - ML FEATURE STORE (Churn Prediction)
=================================================
Pre-computed features for customer churn ML model

Features:
- Customer tenure and engagement
- Transaction recency, frequency, monetary value
- Product holding diversity
- AML alert history
- Channel preferences

Interview talking points:
- "Feature store enables consistent features across training and inference"
- "Pre-aggregation reduces model serving latency from seconds to milliseconds"
- "Features versioned in Delta Lake for reproducibility"
- "Could integrate with SageMaker Feature Store in production"
*/

WITH customer_features AS (
    SELECT
        customer_id,
        
        -- Demographics (encoded)
        age_years,
        CASE age_band
            WHEN 'YOUNG' THEN 1
            WHEN 'MIDDLE_AGED' THEN 2
            WHEN 'MATURE' THEN 3
            ELSE 4
        END AS age_band_encoded,
        
        CASE customer_segment
            WHEN 'RETAIL' THEN 1
            WHEN 'SME' THEN 2
            WHEN 'CORPORATE' THEN 3
            ELSE 4
        END AS segment_encoded,
        
        CAST(customer_tenure_years * 365 AS INT) AS customer_tenure_days,
        risk_score,
        
        -- Account features
        total_accounts,
        active_accounts,
        dormant_accounts,
        CAST(dormant_accounts AS DOUBLE) / NULLIF(total_accounts, 0) AS dormant_ratio,
        
        total_balance_aed,
        total_assets_aed,
        total_liabilities_aed,
        currency_diversity,
        
        -- Product diversification score
        (savings_accounts + checking_accounts + credit_cards + 
         personal_loans + mortgages) AS product_count,
        
        -- Transaction behavior (RFM model)
        DATEDIFF(CURRENT_DATE(), last_transaction_timestamp) AS recency_days,
        txn_count_90d AS frequency_90d,
        total_txn_volume_90d AS monetary_90d,
        
        txn_active_days_90d,
        CAST(txn_active_days_90d AS DOUBLE) / 90.0 AS activity_rate,
        
        avg_txn_amount_90d,
        CAST(high_value_txn_count_90d AS DOUBLE) / NULLIF(txn_count_90d, 0) AS high_value_txn_ratio,
        
        -- Channel behavior
        CAST(atm_usage_90d AS DOUBLE) / NULLIF(txn_count_90d, 0) AS atm_preference,
        CAST(retail_txn_90d AS DOUBLE) / NULLIF(txn_count_90d, 0) AS retail_preference,
        CAST(travel_txn_90d AS DOUBLE) / NULLIF(txn_count_90d, 0) AS travel_preference,
        
        -- Risk indicators
        total_aml_alerts,
        critical_alerts,
        aml_risk_indicator,
        
        -- Engagement
        total_engagement_score,
        CASE engagement_tier
            WHEN 'HIGHLY_ENGAGED' THEN 4
            WHEN 'MODERATELY_ENGAGED' THEN 3
            WHEN 'LOW_ENGAGEMENT' THEN 2
            ELSE 1
        END AS engagement_encoded,
        
        -- Revenue
        estimated_annual_revenue_aed,
        
        -- Target variable (churn within next 90 days)
        CASE churn_risk
            WHEN 'HIGH_CHURN_RISK' THEN 1
            ELSE 0
        END AS churn_target
        
    FROM {{ ref('customer_360') }}
),

-- Add derived features
feature_engineered AS (
    SELECT
        *,
        
        -- Interaction features
        total_balance_aed * product_count AS balance_diversity_interaction,
        recency_days * frequency_90d AS recency_frequency_interaction,
        
        -- Ratio features
        CASE WHEN total_balance_aed > 0 
             THEN estimated_annual_revenue_aed / total_balance_aed 
             ELSE 0 
        END AS revenue_to_balance_ratio,
        
        -- Stability indicators
        CASE
            WHEN recency_days <= 7 THEN 1
            WHEN recency_days <= 30 THEN 0.7
            WHEN recency_days <= 90 THEN 0.3
            ELSE 0
        END AS recency_score,
        
        CASE
            WHEN frequency_90d >= 30 THEN 1
            WHEN frequency_90d >= 15 THEN 0.7
            WHEN frequency_90d >= 5 THEN 0.3
            ELSE 0
        END AS frequency_score,
        
        -- Churn indicators
        CASE WHEN recency_days > 90 AND frequency_90d < 5 THEN 1 ELSE 0 END AS inactive_flag,
        CASE WHEN dormant_accounts > active_accounts THEN 1 ELSE 0 END AS dormancy_flag,
        CASE WHEN total_aml_alerts > 3 THEN 1 ELSE 0 END AS high_alert_flag
        
    FROM customer_features
)

SELECT
    customer_id,
    
    -- All features for ML model
    age_years,
    age_band_encoded,
    segment_encoded,
    customer_tenure_days,
    risk_score,
    total_accounts,
    active_accounts,
    dormant_ratio,
    total_balance_aed,
    currency_diversity,
    product_count,
    recency_days,
    frequency_90d,
    monetary_90d,
    activity_rate,
    avg_txn_amount_90d,
    high_value_txn_ratio,
    atm_preference,
    retail_preference,
    travel_preference,
    aml_risk_indicator,
    engagement_encoded,
    estimated_annual_revenue_aed,
    balance_diversity_interaction,
    recency_frequency_interaction,
    revenue_to_balance_ratio,
    recency_score,
    frequency_score,
    inactive_flag,
    dormancy_flag,
    high_alert_flag,
    
    -- Target variable
    churn_target,
    
    -- Feature version tracking
    CURRENT_TIMESTAMP() AS feature_timestamp,
    'v1.0' AS feature_version

FROM feature_engineered