{% macro spark__datediff(first_date, second_date, datepart) %}
    -- dispatch here gets very very confusing
    -- we just need to hint to dbt that this is a required macro for resolving dbt.spark__datediff()
    -- {{ assert_not_null() }}
    {{ return(adapter.dispatch('datediff', 'dbt')(first_date, second_date, datepart)) }}
{% endmacro %}
