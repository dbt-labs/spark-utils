{{ config(enabled=target.type in ['databricks', 'spark']) }}

{% call spark_only_model() %}
select
    abs(unix_timestamp() - unix_timestamp({{ spark_utils.spark__current_timestamp() }})) < 300 as actual,
    true as expected
union all
-- current_timestamp_in_utc is epoch seconds
select
    abs(unix_timestamp() - {{ spark_utils.spark__current_timestamp_in_utc() }}) < 300 as actual,
    true as expected
{% endcall %}
