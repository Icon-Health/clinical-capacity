{{
    config(
        materialized='incremental'
    )
}}

select
*
from {{ref('demand_final_presentation_week_12')}}
