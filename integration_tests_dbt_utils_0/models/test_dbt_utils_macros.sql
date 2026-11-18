-- Called through the `dbt_utils` namespace (dbt-utils 0.9.6), so they resolve to spark_utils' implementations
select 'concat' as name,
    {{ dbt_utils.concat(["'a'", "'b'", "'c'"]) }} as actual,
    'abc' as expected
union all
select 'split_part',
    {{ dbt_utils.split_part("'a,b,c'", "','", 2) }},
    'b'
union all
select 'split_part_special_char',
    {{ dbt_utils.split_part("'a.b.c'", "'.'", 3) }},
    'c'
