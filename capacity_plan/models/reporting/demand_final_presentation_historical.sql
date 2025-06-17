{{
    config(
        materialized='incremental'
    )
}}

select
'{{ var('identifier') }}' as id,
*,
'{{ var('run_by') }}' as `Run By`,
'{{ var('run_notes') }}' as `Run Notes`
from {{ref('demand_final_presentation')}}
where `Run Type` = 'Forecast'
