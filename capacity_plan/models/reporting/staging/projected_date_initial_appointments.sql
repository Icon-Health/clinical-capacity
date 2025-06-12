select
    reporting_date,
    date_add (
        reporting_date,
        interval time_to_first_appt_8_wk_avg day
    ) as projected_appointment_date,
    run_type,
    dimension,
    dimension_value,
    projected_engaged_referral_lower_bound_count as projected_lower_bound_initial_appointments,
    projected_engaged_referral_count as projected_initial_appointments,
    projected_engaged_referral_upper_bound_count as projected_upper_bound_initial_appointments,
    time_to_first_appt_8_wk_avg
from {{ref('demand_assumptions_considered')}}
where
    run_type = 'Forecast'
