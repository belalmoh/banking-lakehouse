-- =============================================================================
-- Macro: Custom Data Quality Test Macros
-- Reusable quality checks for banking data validation
-- =============================================================================

-- Test: Ensure a numeric column is within a specified range
{% macro test_value_in_range(model, column_name, min_value, max_value) %}
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} < {{ min_value }}
       OR {{ column_name }} > {{ max_value }}
{% endmacro %}

-- Test: Ensure no duplicate records based on composite key
{% macro test_composite_unique(model, columns) %}
    SELECT
        {{ columns | join(', ') }},
        COUNT(*) AS _count
    FROM {{ model }}
    GROUP BY {{ columns | join(', ') }}
    HAVING COUNT(*) > 1
{% endmacro %}

-- Test: Ensure referential integrity between two models
{% macro test_referential_integrity(child_model, child_column, parent_model, parent_column) %}
    SELECT c.{{ child_column }}
    FROM {{ child_model }} c
    LEFT JOIN {{ parent_model }} p ON c.{{ child_column }} = p.{{ parent_column }}
    WHERE p.{{ parent_column }} IS NULL
      AND c.{{ child_column }} IS NOT NULL
{% endmacro %}

-- Test: Ensure data freshness (records within N hours)
{% macro test_data_freshness(model, timestamp_column, max_age_hours) %}
    SELECT
        MAX({{ timestamp_column }}) AS latest_record,
        CURRENT_TIMESTAMP() AS check_time,
        ROUND(
            (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) - UNIX_TIMESTAMP(MAX({{ timestamp_column }}))) / 3600,
            1
        ) AS age_hours
    FROM {{ model }}
    HAVING age_hours > {{ max_age_hours }}
{% endmacro %}

-- Test: Ensure classification columns are present and valid
{% macro test_data_classification(model) %}
    SELECT *
    FROM {{ model }}
    WHERE _data_classification IS NULL
       OR _data_classification NOT IN ('PII', 'CONFIDENTIAL', 'PUBLIC')
       OR _data_residency IS NULL
{% endmacro %}
