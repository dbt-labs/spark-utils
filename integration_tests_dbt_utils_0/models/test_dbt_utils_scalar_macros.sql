select 'cast_array_to_string' as name,
    {{ dbt_utils.cast_array_to_string('array(1, 2, 3)') }} as actual,
    '[1,2,3]' as expected
union all
select 'dateadd',
    cast({{ dbt_utils.dateadd('day', 1, "timestamp '2020-01-01 00:00:00'") }} as string),
    '2020-01-02 00:00:00'
union all
select 'assert_not_null',
    cast({{ assert_not_null('abs', '-5') }} as string),
    '5'
