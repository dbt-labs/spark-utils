select
    {{ spark_utils.spark__split_part("'a,b,c'", "','", 1) }} as actual,
    'a' as expected
union all
select
    {{ spark_utils.spark__split_part("'a,b,c'", "','", 3) }} as actual,
    'c' as expected
