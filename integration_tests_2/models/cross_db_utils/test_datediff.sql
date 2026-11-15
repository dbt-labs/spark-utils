select
    {{ spark_utils.spark__datediff("timestamp '2020-01-01 00:00:00'", "timestamp '2020-01-11 00:00:00'", 'day') }} as actual,
    10 as expected
union all
select
    {{ spark_utils.spark__datediff("timestamp '2020-01-01 00:00:00'", "timestamp '2020-04-01 00:00:00'", 'month') }} as actual,
    3 as expected
