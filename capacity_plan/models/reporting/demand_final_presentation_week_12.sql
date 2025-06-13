select
    `Reporting Week`,
    `Dimension`,
    `Dimension Value`,
    `Appointment Demand Lower Bound Virtual`,
    `Appointment Demand Lower Bound In Person`,
    `Appointment Demand Virtual`,
    `Appointment Demand In Person`,
    `Appointment Demand Upper Bound Virtual`,
    `Appointment Demand Upper Bound In Person`,
    `Demand Hours Lower Bound`,
    `Demand Hours`,
    `Demand Hours Upper Bound`,
     current_date as `Run Date`,
     '{{ var('run_by') }}' as `Run By`,
     '{{ var('run_notes') }}' as `Run Notes`
from
    {{ref('demand_final_presentation')}}
where
    `Forecast Week Sequence` = 12
    and `Run Type` = 'Forecast'
