select
    d.*
except
(attribute_id, forecast_week_sequence),
s.provider_counts,
s.supply_hours_available_actuals,
s.supply_hours_available_ideal,
s.supply_hours_available_actuals - d.demand_hours_lower_bound as net_hours_needed_actuals_lower,
s.supply_hours_available_actuals - d.demand_hours as net_hours_needed_actuals,
s.supply_hours_available_actuals - d.demand_hours_upper_bound as net_hours_needed_actuals_upper,
s.supply_hours_available_ideal - d.demand_hours_lower_bound as net_hours_needed_ideal_lower,
s.supply_hours_available_ideal - d.demand_hours as net_hours_ideal_actuals,
s.supply_hours_available_ideal - d.demand_hours_upper_bound as net_hours_needed_ideal_upper,
d.forecast_week_sequence
from
    {{ref('demand_final')}} d
    left join {{ref('supply_hours')}} s on d.attribute_id = s.attribute_id
where
    d.run_type = 'Forecast'
order by
    d.reporting_week asc
