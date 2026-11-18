select 'dateadd_day' as name,
    cast({{ dbt_utils.dateadd('day', 1, "timestamp '2020-01-01 00:00:00'") }} as string) as actual,
    '2020-01-02 00:00:00' as expected
union all
select 'dateadd_month',
    cast({{ dbt_utils.dateadd('month', 2, "timestamp '2020-01-15 00:00:00'") }} as string),
    '2020-03-15 00:00:00'
union all
select 'datediff_day',
    cast({{ dbt_utils.datediff("date '2020-01-01'", "date '2020-01-11'", 'day') }} as string),
    '10'
union all
select 'type_numeric',
    cast(cast(1.5 as {{ dbt_utils.type_numeric() }}) as string),
    '1.500000'
union all
select 'assert_not_null',
    cast({{ assert_not_null('abs', '-5') }} as string),
    '5'
