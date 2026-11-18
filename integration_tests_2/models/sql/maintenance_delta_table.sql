{{ config(enabled=target.type in ['databricks', 'spark']) }}

{{ config(materialized='table') }}

select
    1 as id,
    'a' as name,
    array(1, 2) as an_array,
    named_struct('k', 'v') as a_struct
