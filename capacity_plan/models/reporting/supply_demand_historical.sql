{{
    config(
        materialized='incremental'
    )
}}

select
'{{ var('identifier') }}' as id,
*,
'{{ var('run_by') }}' as run_by,
'{{ var('run_notes') }}' as run_notes
from {{ref('supply_demand')}}
