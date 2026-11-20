{% macro spark__regexp_instr(source_value, regexp, position=1, occurrence=1, is_raw=False) %}
regexp_instr({{ source_value }}, '{{ regexp }}')
{% endmacro %}