{#- Run with: dbt run-operation test_maintenance_macros
    Exercises the maintenance macros against the delta table built by `maintenance_delta_table`
    and fails the run if any of them misbehaves. -#}
{% macro test_maintenance_macros() %}

    {%- set table_name = target.schema ~ '.maintenance_delta_table' -%}
    {%- set pattern = '.*' ~ target.schema ~ '\\.maintenance_.*' -%}

    {%- set tables = spark_utils.get_tables(pattern) | map('trim') | list -%}
    {%- if table_name not in tables -%}
        {{ exceptions.raise_compiler_error("get_tables did not find " ~ table_name ~ ", got: " ~ tables) }}
    {%- endif -%}
    {%- if (target.schema ~ '.maintenance_view') in tables -%}
        {{ exceptions.raise_compiler_error("get_tables must not return views, got: " ~ tables) }}
    {%- endif -%}

    {%- set columns = spark_utils.get_statistic_columns(table_name) -%}
    {%- if columns | sort | list != ['`id`', '`name`'] -%}
        {{ exceptions.raise_compiler_error("get_statistic_columns should skip array/struct columns, got: " ~ columns) }}
    {%- endif -%}

    {{ spark_utils.spark_optimize_delta_tables(pattern) }}
    {{ spark_utils.spark_vacuum_delta_tables(pattern) }}
    {{ spark_utils.spark_analyze_tables(pattern) }}

    {{ log("maintenance macros OK", info=True) }}

{% endmacro %}
