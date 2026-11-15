select
    {{ spark_utils.spark__concat(["'a'", "'b'", "'c'"]) }} as actual,
    'abc' as expected
