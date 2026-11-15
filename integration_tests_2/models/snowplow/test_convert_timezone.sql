select
    {{ spark_utils.spark__convert_timezone("'UTC'", "'America/New_York'", "timestamp '2020-01-01 12:00:00'") }} as actual,
    timestamp '2020-01-01 07:00:00' as expected
union all
select
    {{ spark_utils.spark__convert_timezone("'America/New_York'", "'UTC'", "timestamp '2020-01-01 07:00:00'") }} as actual,
    timestamp '2020-01-01 12:00:00' as expected
