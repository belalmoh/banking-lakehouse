-- =============================================================================
-- Macro: Currency Conversion
-- Converts amounts between currencies using fixed rates (demo purposes)
-- In production, this would connect to a live FX rate API/table
-- =============================================================================

{% macro convert_currency(amount_expr, from_currency_expr, to_currency_expr) %}
    CASE
        -- AED conversion rates (approximate)
        WHEN {{ from_currency_expr }} = 'AED' AND {{ to_currency_expr }} = 'USD' THEN ROUND({{ amount_expr }} * 0.2723, 2)
        WHEN {{ from_currency_expr }} = 'AED' AND {{ to_currency_expr }} = 'EUR' THEN ROUND({{ amount_expr }} * 0.2510, 2)
        WHEN {{ from_currency_expr }} = 'AED' AND {{ to_currency_expr }} = 'GBP' THEN ROUND({{ amount_expr }} * 0.2157, 2)
        -- USD conversion rates
        WHEN {{ from_currency_expr }} = 'USD' AND {{ to_currency_expr }} = 'AED' THEN ROUND({{ amount_expr }} * 3.6725, 2)
        WHEN {{ from_currency_expr }} = 'USD' AND {{ to_currency_expr }} = 'EUR' THEN ROUND({{ amount_expr }} * 0.9217, 2)
        WHEN {{ from_currency_expr }} = 'USD' AND {{ to_currency_expr }} = 'GBP' THEN ROUND({{ amount_expr }} * 0.7924, 2)
        -- EUR conversion rates
        WHEN {{ from_currency_expr }} = 'EUR' AND {{ to_currency_expr }} = 'AED' THEN ROUND({{ amount_expr }} * 3.9840, 2)
        WHEN {{ from_currency_expr }} = 'EUR' AND {{ to_currency_expr }} = 'USD' THEN ROUND({{ amount_expr }} * 1.0849, 2)
        WHEN {{ from_currency_expr }} = 'EUR' AND {{ to_currency_expr }} = 'GBP' THEN ROUND({{ amount_expr }} * 0.8598, 2)
        -- GBP conversion rates
        WHEN {{ from_currency_expr }} = 'GBP' AND {{ to_currency_expr }} = 'AED' THEN ROUND({{ amount_expr }} * 4.6340, 2)
        WHEN {{ from_currency_expr }} = 'GBP' AND {{ to_currency_expr }} = 'USD' THEN ROUND({{ amount_expr }} * 1.2620, 2)
        WHEN {{ from_currency_expr }} = 'GBP' AND {{ to_currency_expr }} = 'EUR' THEN ROUND({{ amount_expr }} * 1.1630, 2)
        -- Same currency: no conversion
        WHEN {{ from_currency_expr }} = {{ to_currency_expr }} THEN ROUND({{ amount_expr }}, 2)
        ELSE ROUND({{ amount_expr }}, 2)
    END
{% endmacro %}
