select
    {{ spark_utils.spark__array_append('array(1, 2)', 3) }} as actual,
    array(1, 2, 3) as expected
