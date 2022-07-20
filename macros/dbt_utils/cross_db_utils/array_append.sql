{% macro spark__array_append(array, new_element) -%}
    {{ dbt_utils.array_concat(array, dbt_utils.array_construct([new_element])) }}
{%- endmacro %}