{% macro spark__datediff(first_date, second_date, datepart) %}
    {{ return(dbt.datediff(first_date, second_date, datepart)) }}
{% endmacro %}
