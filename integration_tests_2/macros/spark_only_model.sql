{#- Wrap the body of a test model that calls Spark-only macros:
        {% call spark_only_model() %} ... {% endcall %}
    dbt-core renders every model at parse time, even disabled ones, so on other adapters
    (e.g. the DuckDB parse jobs) the body is never rendered and a placeholder is returned. -#}
{% macro spark_only_model() -%}
    {%- if target.type in ['databricks', 'spark'] -%}
        {{ caller() }}
    {%- else -%}
        select 1 as actual, 1 as expected
    {%- endif -%}
{%- endmacro %}
