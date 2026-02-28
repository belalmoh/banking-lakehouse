{#
    Custom macro to read Bronze Delta tables directly from S3 paths.
    This avoids needing a persistent Hive metastore.
    
    Usage in models:
        SELECT * FROM {{ bronze_table('customers') }}
        SELECT * FROM {{ bronze_table('transactions') }}
#}

{% macro bronze_table(table_name) %}
    delta.`s3a://bronze/{{ table_name }}`
{% endmacro %}
