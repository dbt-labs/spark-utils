{{ config(enabled=target.type in ['databricks', 'spark']) }}

{% call spark_only_model() %}
select
    {{ spark_utils.spark__dateadd('day', 1, "timestamp '2020-01-01 00:00:00'") }} as actual,
    timestamp '2020-01-02 00:00:00' as expected
union all
select
    {{ spark_utils.spark__dateadd('month', 2, "timestamp '2020-01-15 00:00:00'") }} as actual,
    timestamp '2020-03-15 00:00:00' as expected
{% endcall %}
