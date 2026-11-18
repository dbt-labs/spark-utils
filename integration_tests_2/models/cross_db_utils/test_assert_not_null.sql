{{ config(enabled=target.type in ['databricks', 'spark']) }}

{% call spark_only_model() %}
select
    {{ spark_utils.assert_not_null('abs', '-5') }} as actual,
    5 as expected
union all
select
    {{ spark_utils.assert_not_null('upper', "'abc'") }} as actual,
    'ABC' as expected
{% endcall %}
