{{ config(enabled=target.type in ['databricks', 'spark']) }}

{% call spark_only_model() %}
select
    {{ spark_utils.spark__array_concat('array(1, 2)', 'array(3)') }} as actual,
    array(1, 2, 3) as expected
union all
select
    {{ spark_utils.spark__array_concat('array(1)', 'array()') }} as actual,
    array(1) as expected
{% endcall %}
