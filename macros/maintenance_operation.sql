{% macro get_delta_tables() %}
  {% set delta_tables = [] %}
  {% for database in spark__list_schemas('not_used') %}
    {% for table in spark__list_relations_without_caching(database[0]) %}
      {% set db_tablename = database[0] ~ "." ~ table[1] %}
      {% call statement('table_detail', fetch_result=True) -%}
        describe extended {{ db_tablename }}
      {% endcall %}

      {% for row in load_result('table_detail').table %}
        {% if row[0]|lower == 'provider' and row[1]|lower == 'delta' %}
          {{ delta_tables.append(db_tablename) }}
        {% endif %}
      {% endfor %}
    {% endfor %}
  {% endfor %}
  {{ return(delta_tables) }}
{% endmacro %}

{% macro get_tables() %}
  {% set tables = [] %}
  {% for database in spark__list_schemas('not_used') %}
    {% for table in spark__list_relations_without_caching(database[0]) %}
      {% set db_tablename = database[0] ~ "." ~ table[1] %}
      {% call statement('table_detail', fetch_result=True) -%}
        describe extended {{ db_tablename }}
      {% endcall %}

      {% for row in load_result('table_detail').table %}
        {% if row[0]|lower == 'type' and row[1]|lower != 'view' %}
        {{ tables.append(db_tablename) }}
        {% endif %}
      {% endfor %}
    {% endfor %}
  {% endfor %}
  {{ return(tables) }}
{% endmacro %}

{% macro get_statistic_columns(table) %}

  {% call statement('input_columns', fetch_result=True) %}
      SHOW COLUMNS IN {{ table }}
  {% endcall %}
  {% set input_columns = load_result('input_columns').table %}

  {% set output_columns = [] %}
  {% for column in input_columns %}
    {% call statement('column_information', fetch_result=True) %}
        DESCRIBE TABLE {{ table }} {{ column[0] }}
    {% endcall %}
    {% if not load_result('column_information').table[1][1].startswith('struct') and not load_result('column_information').table[1][1].startswith('array')  %}
      {{ output_columns.append(column[0]) }}
    {% endif %}
  {% endfor %}
  {{ return(output_columns) }}
{% endmacro %}

{% macro spark_maintenance() %}

    {% for table in get_delta_tables() %}
        {% set start=modules.datetime.datetime.now() %}
        {% set message_prefix=loop.index ~ " of " ~ loop.length %}
        {{ dbt_utils.log_info(message_prefix ~ " Optimizing " ~ table) }}
        {% do run_query("optimize " ~ table) %}
        {% set end=modules.datetime.datetime.now() %}
        {% set total_seconds = (end - start).total_seconds() | round(2)  %}
        {{ dbt_utils.log_info(message_prefix ~ " Finished " ~ table ~ " in " ~ total_seconds ~ "s") }}
    {% endfor %}

{% endmacro %}

{% macro spark_statistics() %}

    {% for table in get_tables() %}
        {% set start=modules.datetime.datetime.now() %}
        {% set columns = get_statistic_columns(table) | join(',') %}
        {% set message_prefix=loop.index ~ " of " ~ loop.length %}
        {% if columns != '' %}
            {% do run_query("analyze table " ~ table ~ " compute statistics for columns " ~ columns) %}
        {% endif %}
        {% set end=modules.datetime.datetime.now() %}
        {% set total_seconds = (end - start).total_seconds() | round(2)  %}
        {{ dbt_utils.log_info(message_prefix ~ " Finished " ~ table ~ " in " ~ total_seconds ~ "s") }}
    {% endfor %}

{% endmacro %}
