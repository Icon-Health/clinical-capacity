with
    appointment_demand_staging as (
        select
            d.*,
            af.patients_with_two_plus_appointment_pct,
            d.projected_lower_bound_initial_appointments * af.patients_with_two_plus_appointment_pct as projected_lower_bound_follow_up_appointments_in_que,
            d.projected_initial_appointments * af.patients_with_two_plus_appointment_pct as projected_followup_appointments_in_que,
            d.projected_upper_bound_initial_appointments * af.patients_with_two_plus_appointment_pct as projected_upper_bound_follow_up_appointments_in_que,
            fa.time_to_followup_appt_8_wk_avg,
            faf.projected_lower_bound_follow_up_appointments,
            faf.projected_followup_appointments,
            faf.projected_upper_bound_follow_up_appointments,
            --faf.projected_lower_bound_initial_appointments + projected_lower_bound_follow_up_appointments as appointment_demand_lower_bound,
            --faf.projected_initial_appointments + projected_followup_appointments as appointment_demand,
            --faf.projected_upper_bound_initial_appointments + projected_upper_bound_follow_up_appointments as appointment_demand_upper_bound
        from
            {{ref('demand_assumptions_cosidered_2')}} d
            left join {{ref('appointment_frequency_bin')}} af on d.attribute_id = af.attribute_id
            left join {{ref('time_to_followup_appt_8_wk_avg')}} fa on d.attribute_id = fa.attribute_id
            left join {{ref('projected_date_followup_appointments')}} faf on d.reporting_date = faf.projected_date_followup_appointments
        order by
            reporting_date asc
    ),
    virual_in_person_logic as (
        select
            a.*
        except
        (attribute_id),
        i.initial_appt_virtual_pct,
        i.initial_appt_in_person_pct,
        --5.2459893048128343
        a.projected_lower_bound_initial_appointments * i.initial_appt_virtual_pct as projected_lower_bound_initial_appointments_virtual,
        a.projected_lower_bound_initial_appointments * i.initial_appt_in_person_pct as projected_lower_bound_initial_appointments_in_person,
        --
        a.projected_initial_appointments * i.initial_appt_virtual_pct as projected_initial_appointments_virtual,
        a.projected_initial_appointments * i.initial_appt_in_person_pct as projected_initial_appointments_in_person,
        --
        a.projected_upper_bound_initial_appointments * i.initial_appt_virtual_pct as projected_upper_bound_initial_appointments_virtual,
        a.projected_upper_bound_initial_appointments * i.initial_appt_in_person_pct as projected_upper_bound_initial_appointments_in_person,
        --
        ff.followup_appt_virtual_pct,
        ff.followup_appt_in_person_pct,
        a.projected_lower_bound_follow_up_appointments * ff.followup_appt_virtual_pct as projected_lower_bound_follow_up_appointments_virtual,
        a.projected_lower_bound_follow_up_appointments * ff.followup_appt_in_person_pct as projected_lower_bound_follow_up_appointments_in_person,
        --
        a.projected_followup_appointments * ff.followup_appt_virtual_pct as projected_followup_appointments_virtual,
        a.projected_followup_appointments * ff.followup_appt_in_person_pct as projected_followup_appointments_in_person,
        --
        a.projected_upper_bound_follow_up_appointments * ff.followup_appt_virtual_pct as projected_upper_bound_follow_up_appointments_virtual,
        a.projected_upper_bound_follow_up_appointments * ff.followup_appt_in_person_pct as projected_upper_bound_follow_up_appointments_in_person,
        --
        from
            appointment_demand_staging a
            left join {{ref('inital_appointment_mode_distribution')}} i on a.attribute_id = i.attribute_id
            left join {{ref('followup_appointment_mode_distribution')}} ff on a.attribute_id = ff.attribute_id
    ),
    demand_totals as (
        select
            *,
            projected_lower_bound_initial_appointments_virtual + projected_lower_bound_follow_up_appointments_virtual as appointment_demand_lower_bound_virtual,
            projected_lower_bound_initial_appointments_in_person + projected_lower_bound_follow_up_appointments_in_person as appointment_demand_lower_bound_in_person,
            projected_initial_appointments_virtual + projected_followup_appointments_virtual as appointment_demand_virtual,
            projected_initial_appointments_in_person + projected_followup_appointments_in_person as appointment_demand_in_person,
            projected_upper_bound_initial_appointments_virtual + projected_upper_bound_follow_up_appointments_virtual as appointment_demand_upper_bound_virtual,
            projected_upper_bound_initial_appointments_in_person + projected_upper_bound_follow_up_appointments_in_person as appointment_demand_upper_bound_in_person,
        from
            virual_in_person_logic
    )
select
    *,
    (
        (
            cast(appointment_demand_lower_bound_virtual as int64) * 45
        ) + (
            cast(appointment_demand_lower_bound_in_person as int64) * 60
        )
    ) / 60 as demand_hours_lower_bound,
    (
        (cast(appointment_demand_virtual as int64) * 45) + (cast(appointment_demand_in_person as int64) * 60)
    ) / 60 as demand_hours,
    (
        (
            cast(appointment_demand_upper_bound_virtual as int64) * 45
        ) + (
            cast(appointment_demand_upper_bound_in_person as int64) * 60
        )
    ) / 60 as demand_hours_upper_bound
from
    demand_totals
order by
    reporting_date asc
