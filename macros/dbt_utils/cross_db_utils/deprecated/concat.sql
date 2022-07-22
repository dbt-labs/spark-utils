{% macro spark__concat(fields) -%}
    {{ return(adapter.dispatch('concat', 'dbt')(fields)) }}
{%- endmacro %}
