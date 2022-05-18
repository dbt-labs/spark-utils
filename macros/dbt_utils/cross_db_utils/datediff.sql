{% macro spark__datediff(first_date, second_date, datepart) %}
    {{ return(adapter.dispatch('dateadd', 'dbt')(datepart, interval, from_date_or_timestamp)) }}
{% endmacro %}
