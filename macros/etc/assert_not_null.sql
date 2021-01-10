{% macro assert_not_null(function, arg) %}

    coalesce({{function}}({{arg}}), assert_true({{function}}({{arg}}) is not null))

{% endmacro %}
