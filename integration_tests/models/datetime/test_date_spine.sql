{{ config(materialized='table') }}

with date_spine as (

        {{ spark_utils.date_spine("day", "'2018-01-01'", "'2018-01-10'") }}

)

select date_day
from date_spine
