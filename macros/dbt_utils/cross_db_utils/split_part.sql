{% macro spark__split_part(string_text, delimiter_text, part_number) %}
  {{ return(dbt.split_part(string_text, delimiter_text, part_number)) }}
{% endmacro %}
