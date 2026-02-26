-- =============================================================================
-- Macro: Calculate Risk Score
-- Composite risk scoring combining KYC risk, AML alerts, and transaction flags
-- =============================================================================

{% macro calculate_risk_score(risk_score_col, aml_alert_count_col, flagged_txn_count_col) %}
    ROUND(
        -- Base KYC risk (40% weight)
        (CAST({{ risk_score_col }} AS DOUBLE) * 0.40) +
        -- AML alert intensity (35% weight, capped at 100)
        (LEAST(CAST({{ aml_alert_count_col }} AS DOUBLE) * 10, 100) * 0.35) +
        -- Flagged transaction frequency (25% weight, capped at 100)
        (LEAST(CAST({{ flagged_txn_count_col }} AS DOUBLE) * 5, 100) * 0.25),
        2
    )
{% endmacro %}
