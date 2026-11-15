select
    {{ spark_utils.spark__array_construct([1, 2, 3]) }} as actual,
    array(1, 2, 3) as expected
union all
select
    {{ spark_utils.spark__array_construct(["'a'", "'b'"]) }} as actual,
    array('a', 'b') as expected
