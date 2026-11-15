{% macro spark__array_append(array, new_element) -%}
    {{ spark_utils.spark__array_concat(array, spark_utils.spark__array_construct([new_element])) }}
{%- endmacro %}