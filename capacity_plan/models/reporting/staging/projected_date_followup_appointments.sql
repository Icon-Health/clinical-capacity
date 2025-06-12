with
    staging as (
        select
            d.reporting_date,
            date_add (
                d.reporting_date,
                interval time_to_followup_appt_8_wk_avg day
            ) as projected_date_followup_appointments,
            d.run_type,
            d.dimension,
            d.dimension_value,
            d.projected_lower_bound_initial_appointments,
            d.projected_initial_appointments,
            d.projected_upper_bound_initial_appointments,
            af.patients_with_two_plus_appointment_pct,
            d.projected_lower_bound_initial_appointments * af.patients_with_two_plus_appointment_pct as projected_lower_bound_follow_up_appointments,
            d.projected_initial_appointments * af.patients_with_two_plus_appointment_pct as projected_followup_appointments,
            d.projected_upper_bound_initial_appointments * af.patients_with_two_plus_appointment_pct as projected_upper_bound_follow_up_appointments,
            fa.time_to_followup_appt_8_wk_avg
        from
            {{ref('demand_assumptions_cosidered_2')}} d
            left join {{ref('appointment_frequency_bin')}} af on d.attribute_id = af.attribute_id
            left join {{ref('time_to_followup_appt_8_wk_avg')}} fa on d.attribute_id = fa.attribute_id
        order by
            reporting_date asc
    )
select
    *
from
    staging
where
    projected_date_followup_appointments >= reporting_date
