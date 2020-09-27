{% macro spark__split_part(string_text, delimiter_text, part_number) %}

    split(
        {{ string_text }},
        concat('\\', {{ delimiter_text }})
        )[({{ part_number - 1 }})]

{% endmacro %}
