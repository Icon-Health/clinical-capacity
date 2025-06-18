select
d.* except(forecast_week_sequence,run_by,run_notes),
a.* except(appointment_week),
forecast_week_sequence
from {{ref('supply_demand_historical')}} d
left join {{ref('appointment_actuals_week')}} a
     on d.reporting_week = a.appointment_week
order by forecast_week_sequence asc
