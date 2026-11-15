{{ config(materialized='view') }}

select * from {{ ref('maintenance_delta_table') }}
