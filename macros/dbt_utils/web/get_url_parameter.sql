{#- dbt_utils.get_url_parameter uses split_part. On Spark, split_part treats the delimiter as a
    regular expression and escapes it when it starts with a special character, so a parameter such
    as 'gclid=' becomes the invalid pattern '\gclid='. regexp_extract avoids that. -#}
{% macro spark__get_url_parameter(field, url_parameter) -%}

{%- set formatted_url_parameter = "'" + url_parameter + "=([^&]+)'" -%}

nullif(regexp_extract({{ field }}, {{ formatted_url_parameter }}, 1), '')

{%- endmacro %}
