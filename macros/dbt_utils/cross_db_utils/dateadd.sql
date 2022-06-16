{% macro spark__dateadd(datepart, interval, from_date_or_timestamp) %}
    {{ return(dbt.dateadd(datepart, interval, from_date_or_timestamp)) }}
{% endmacro %}
