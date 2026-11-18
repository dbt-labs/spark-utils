{{ config(enabled=target.type in ['databricks', 'spark']) }}

{{ config(materialized='view') }}

select * from {{ ref('maintenance_delta_table') }}
