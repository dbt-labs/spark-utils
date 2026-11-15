{{ config(enabled=target.type in ['databricks', 'spark']) }}

{% call spark_only_model() %}
select
    cast(1.5 as {{ spark_utils.spark__type_numeric() }}) as actual,
    1.5 as expected
{% endcall %}
