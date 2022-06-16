{% macro spark__datediff(first_date, second_date, datepart) %}
    {{ return(dbt.dateadd(datepart, interval, from_date_or_timestamp)) }}
{% endmacro %}
