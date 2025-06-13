with
    staging as (
        SELECT
            date(DATE_TRUNC (reporting_date, WEEK (MONDAY))) AS reporting_week,
            run_type,
            dimension,
            dimension_value,
            SUM(referral_lower_bound_count) AS referral_lower_bound_count,
            SUM(referral_count) AS referral_count,
            SUM(referral_upper_bound_count) AS referral_upper_bound_count,
            MAX(engaged_appt_scheduled_pct_last_8_cohorts_avg) AS engaged_appt_scheduled_pct_last_8_cohorts_avg,
            SUM(projected_engaged_referral_lower_bound_count) AS projected_engaged_referral_lower_bound_count,
            SUM(projected_engaged_referral_count) AS projected_engaged_referral_count,
            SUM(projected_engaged_referral_upper_bound_count) AS projected_engaged_referral_upper_bound_count,
            MAX(time_to_first_appt_8_wk_avg) AS time_to_first_appt_8_wk_avg,
            SUM(projected_lower_bound_initial_appointments) AS projected_lower_bound_initial_appointments,
            SUM(projected_initial_appointments) AS projected_initial_appointments,
            SUM(projected_upper_bound_initial_appointments) AS projected_upper_bound_initial_appointments,
            max(patients_with_two_plus_appointment_pct) AS patients_with_two_plus_appointment_pct,
            SUM(
                projected_lower_bound_follow_up_appointments_in_que
            ) AS projected_lower_bound_follow_up_appointments_in_que,
            SUM(projected_followup_appointments_in_que) AS projected_followup_appointments_in_que,
            SUM(
                projected_upper_bound_follow_up_appointments_in_que
            ) AS projected_upper_bound_follow_up_appointments_in_que,
            MAX(time_to_followup_appt_8_wk_avg) AS time_to_followup_appt_8_wk_avg,
            SUM(projected_lower_bound_follow_up_appointments) AS projected_lower_bound_follow_up_appointments,
            SUM(projected_followup_appointments) AS projected_followup_appointments,
            SUM(projected_upper_bound_follow_up_appointments) AS projected_upper_bound_follow_up_appointments,
            MAX(initial_appt_virtual_pct) AS initial_appt_virtual_pct,
            MAX(initial_appt_in_person_pct) AS initial_appt_in_person_pct,
            SUM(
                projected_lower_bound_initial_appointments_virtual
            ) AS projected_lower_bound_initial_appointments_virtual,
            SUM(
                projected_lower_bound_initial_appointments_in_person
            ) AS projected_lower_bound_initial_appointments_in_person,
            SUM(projected_initial_appointments_virtual) AS projected_initial_appointments_virtual,
            SUM(projected_initial_appointments_in_person) AS projected_initial_appointments_in_person,
            SUM(
                projected_upper_bound_initial_appointments_virtual
            ) AS projected_upper_bound_initial_appointments_virtual,
            SUM(
                projected_upper_bound_initial_appointments_in_person
            ) AS projected_upper_bound_initial_appointments_in_person,
            MAX(followup_appt_virtual_pct) AS followup_appt_virtual_pct,
            MAX(followup_appt_in_person_pct) AS followup_appt_in_person_pct,
            SUM(
                projected_lower_bound_follow_up_appointments_virtual
            ) AS projected_lower_bound_follow_up_appointments_virtual,
            SUM(
                projected_lower_bound_follow_up_appointments_in_person
            ) AS projected_lower_bound_follow_up_appointments_in_person,
            SUM(projected_followup_appointments_virtual) AS projected_followup_appointments_virtual,
            SUM(projected_followup_appointments_in_person) AS projected_followup_appointments_in_person,
            SUM(
                projected_upper_bound_follow_up_appointments_virtual
            ) AS projected_upper_bound_follow_up_appointments_virtual,
            SUM(
                projected_upper_bound_follow_up_appointments_in_person
            ) AS projected_upper_bound_follow_up_appointments_in_person,
            SUM(appointment_demand_lower_bound_virtual) AS appointment_demand_lower_bound_virtual,
            SUM(appointment_demand_lower_bound_in_person) AS appointment_demand_lower_bound_in_person,
            SUM(appointment_demand_virtual) AS appointment_demand_virtual,
            SUM(appointment_demand_in_person) AS appointment_demand_in_person,
            SUM(appointment_demand_upper_bound_virtual) AS appointment_demand_upper_bound_virtual,
            SUM(appointment_demand_upper_bound_in_person) AS appointment_demand_upper_bound_in_person,
            SUM(demand_hours_lower_bound) AS demand_hours_lower_bound,
            SUM(demand_hours) AS demand_hours,
            SUM(demand_hours_upper_bound) AS demand_hours_upper_bound
        FROM
        {{ref('demand_final_daily')}}
        GROUP BY ALL
        UNION ALL
        SELECT
            date(DATE_TRUNC (creation_datetime, WEEK (MONDAY))) AS reporting_week,
            'Actuals' AS run_type,
            'client' AS dimension,
            client AS dimension_value,
            NULL AS referral_lower_bound_count,
            COUNT(DISTINCT ID) AS referral_count,
            NULL AS referral_upper_bound_count,
            NULL AS engaged_appt_scheduled_pct_last_8_cohorts_avg,
            NULL AS projected_engaged_referral_lower_bound_count,
            NULL AS projected_engaged_referral_count,
            NULL AS projected_engaged_referral_upper_bound_count,
            NULL AS time_to_first_appt_8_wk_avg,
            NULL AS projected_lower_bound_initial_appointments,
            NULL AS projected_initial_appointments,
            NULL AS projected_upper_bound_initial_appointments,
            NULL AS patients_with_two_plus_appointment_pct,
            NULL AS projected_lower_bound_follow_up_appointments_in_que,
            NULL AS projected_followup_appointments_in_que,
            NULL AS projected_upper_bound_follow_up_appointments_in_que,
            NULL AS time_to_followup_appt_8_wk_avg,
            NULL AS projected_lower_bound_follow_up_appointments,
            NULL AS projected_followup_appointments,
            NULL AS projected_upper_bound_follow_up_appointments,
            NULL AS initial_appt_virtual_pct,
            NULL AS initial_appt_in_person_pct,
            NULL AS projected_lower_bound_initial_appointments_virtual,
            NULL AS projected_lower_bound_initial_appointments_in_person,
            NULL AS projected_initial_appointments_virtual,
            NULL AS projected_initial_appointments_in_person,
            NULL AS projected_upper_bound_initial_appointments_virtual,
            NULL AS projected_upper_bound_initial_appointments_in_person,
            NULL AS followup_appt_virtual_pct,
            NULL AS followup_appt_in_person_pct,
            NULL AS projected_lower_bound_follow_up_appointments_virtual,
            NULL AS projected_lower_bound_follow_up_appointments_in_person,
            NULL AS projected_followup_appointments_virtual,
            NULL AS projected_followup_appointments_in_person,
            NULL AS projected_upper_bound_follow_up_appointments_virtual,
            NULL AS projected_upper_bound_follow_up_appointments_in_person,
            NULL AS appointment_demand_lower_bound_virtual,
            NULL AS appointment_demand_lower_bound_in_person,
            NULL AS appointment_demand_virtual,
            NULL AS appointment_demand_in_person,
            NULL AS appointment_demand_upper_bound_virtual,
            NULL AS appointment_demand_upper_bound_in_person,
            NULL AS demand_hours_lower_bound,
            NULL AS demand_hours,
            NULL AS demand_hours_upper_bound
        FROM
        {{source('clinical_reporting_pipeline','inbound_referrals')}}
        WHERE
            client IS NOT NULL
            AND ID IS NOT NULL
            AND {{ var('dimension') }} = '{{ var('dimension_value') }}'
            AND DATE(DATE_TRUNC (creation_datetime, WEEK (MONDAY))) < DATE(DATE_TRUNC (CURRENT_DATE(), WEEK (MONDAY)))
        GROUP BY ALL
        order by
            reporting_week asc
    )
select
    *,
    ROW_NUMBER() OVER (
        PARTITION BY
            run_type -- Optional: remove if not grouping by client
        ORDER BY
            reporting_week
    ) AS forecast_week_sequence
from
    staging
