{% macro assert_not_null(function, arg) -%}
  {{ return(adapter.dispatch('assert_not_null', 'dbt')(function, arg)) }}
{%- endmacro %}
