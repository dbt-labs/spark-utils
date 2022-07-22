{# numeric  ------------------------------------------------     #}

{% macro spark__type_numeric() %}
    {{ return(dbt.type_numeric()) }}
{% endmacro %}
