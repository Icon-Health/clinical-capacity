select
d.id as `ID`,
d.reporting_week as `Reporting Week`,
d.run_type as `Run Type`,
d.dimension as `Dimension`,
d.dimension_value as `Dimension Value`,
d.demand_hours_lower_bound as `Demand Hours Lower Bound`,
d.demand_hours as `Demand Hours`,
d.demand_hours_upper_bound as `Demand Hours Upper Bound`,
a.appointment_hours_actuals as `Appointment Hours Actuals`,
forecast_week_sequence as `Forecast Week Sequence`
from {{ref('supply_demand_historical')}} d
left join {{ref('appointment_actuals_week')}} a
     on d.reporting_week = a.appointment_week
order by id, forecast_week_sequence asc
