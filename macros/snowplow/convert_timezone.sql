{% macro spark__convert_timezone(in_tz, out_tz, in_timestamp) %}
    {% if in_tz|lower != "'utc'" %}
        {% do exceptions.raise_compiler_error("Spark can only convert from timestamps in UTC") %}
    {% endif %}
    from_utc_timestamp({{in_timestamp}}, {{out_tz}})
{% endmacro %}
