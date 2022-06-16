{% macro spark__concat(fields) -%}
    {{ return(dbt.concat(fields)) }}
{%- endmacro %}
