{{ config(enabled=target.type in ['databricks', 'spark']) }}

select 1 as id
