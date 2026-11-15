{{ config(enabled=target.type in ['databricks', 'spark']) }}

{% call spark_only_model() %}
select
    {{ spark_utils.spark__array_append('array(1, 2)', 3) }} as actual,
    array(1, 2, 3) as expected
{% endcall %}
