select
    d.*,
    p.projected_lower_bound_initial_appointments,
    p.projected_initial_appointments,
    p.projected_upper_bound_initial_appointments
from
    {{ref('demand_assumptions_considered')}} d
    left join {{ref('projected_date_initial_appointments')}} p on d.reporting_date = p.projected_appointment_date
