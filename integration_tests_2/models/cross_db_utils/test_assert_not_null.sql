select
    {{ spark_utils.assert_not_null('abs', '-5') }} as actual,
    5 as expected
union all
select
    {{ spark_utils.assert_not_null('upper', "'abc'") }} as actual,
    'ABC' as expected
