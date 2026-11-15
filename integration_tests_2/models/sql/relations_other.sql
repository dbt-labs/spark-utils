{{ config(enabled=target.type in ['databricks', 'spark']) }}

select 3 as id
