{% macro spark__dateadd(datepart, interval, from_date_or_timestamp) %}
    -- dispatch here gets very very confusing
    -- we just need to hint to dbt that this is a required macro for resolving dbt.spark__datediff()
    -- {{ assert_not_null() }}
    {{ return(adapter.dispatch('dateadd', 'dbt')(datepart, interval, from_date_or_timestamp)) }}
{% endmacro %}
