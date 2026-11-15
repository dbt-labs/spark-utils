select
    {{ spark_utils.spark__cast_array_to_string('array(1, 2, 3)') }} as actual,
    '[1,2,3]' as expected
union all
select
    {{ spark_utils.spark__cast_array_to_string("array('a', 'b')") }} as actual,
    '[a,b]' as expected
