{# numeric  ------------------------------------------------     #}

{% macro spark__type_numeric() %}
    {{ return(adapter.dispatch('type_numeric', 'dbt')()) }}
{% endmacro %}
