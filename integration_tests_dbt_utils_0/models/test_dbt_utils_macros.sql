-- Called through the `dbt_utils` namespace, so they resolve to spark_utils' implementations
select 'array_concat' as name,
    {{ dbt_utils.array_concat('array(1, 2)', 'array(3)') }} as actual,
    array(1, 2, 3) as expected
union all
select 'array_concat_empty',
    {{ dbt_utils.array_concat('array(1)', 'array()') }},
    array(1)
union all
select 'array_append',
    {{ dbt_utils.array_append('array(1, 2)', 3) }},
    array(1, 2, 3)
union all
select 'array_construct',
    {{ dbt_utils.array_construct([1, 2, 3]) }},
    array(1, 2, 3)
union all
select 'array_construct_strings',
    {{ dbt_utils.array_construct(["'a'", "'b'"]) }},
    array('a', 'b')
