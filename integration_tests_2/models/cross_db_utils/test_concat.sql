{{ config(enabled=target.type in ['databricks', 'spark']) }}

{% call spark_only_model() %}
select
    {{ spark_utils.spark__concat(["'a'", "'b'", "'c'"]) }} as actual,
    'abc' as expected
{% endcall %}
