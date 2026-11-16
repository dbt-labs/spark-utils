{{ config(enabled=target.type in ['databricks', 'spark']) }}

{% call spark_only_model() %}
-- depends_on: {{ ref('relations_prefix_a') }}
-- depends_on: {{ ref('relations_prefix_b') }}
-- depends_on: {{ ref('relations_other') }}

{% if not execute %}

select 'parse time' as macro, 0 as actual, 0 as expected

{% else %}

{%- set schema = target.schema -%}
{%- set by_prefix = dbt_utils.get_relations_by_prefix(schema, 'relations_prefix_') -%}
{%- set by_pattern = dbt_utils.get_relations_by_pattern(schema, 'relations_prefix_*') -%}
{#- dbt-utils 1.x has no get_tables_by_prefix / get_tables_by_pattern to dispatch from, so these two are called directly -#}
{%- set tables_by_prefix = spark_utils.spark__get_tables_by_prefix(schema, 'relations_prefix_') -%}
{%- set tables_by_pattern = spark_utils.spark__get_tables_by_pattern(schema, 'relations_prefix_*') -%}
{%- set all_relations = dbt_utils.get_relations_by_prefix(schema, 'relations_') -%}
{%- set no_match = dbt_utils.get_relations_by_prefix(schema, 'no_such_prefix_') -%}

select 'get_relations_by_prefix' as macro, {{ by_prefix | length }} as actual, 2 as expected
union all
select 'get_relations_by_pattern', {{ by_pattern | length }}, 2
union all
select 'get_tables_by_prefix', {{ tables_by_prefix | length }}, 2
union all
select 'get_tables_by_pattern', {{ tables_by_pattern | length }}, 2
union all
select 'prefix matches every relations_* model', {{ all_relations | length }}, 3
union all
select 'no match returns an empty list', {{ no_match | length }}, 0

{% endif %}
{% endcall %}
