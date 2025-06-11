create
or replace table capacity_plan.followup_appointment_mode_distribution as
with
    appointment_deduped as (
        select
            *
        from
            `clinical_reporting_pipeline.appointments`
        where
            client = 'Conviva' qualify row_number() over (
                partition by
                    patient_id,
                    date(appointment_datetime)
            ) = 1
    ),
    appointment_staging as (
        select
            patient_id,
            date(appointment_datetime) as appointment_date,
            appointment_id,
            appointment_mode,
            duration_minutes,
            row_number() over (
                partition by
                    patient_id
                order by
                    appointment_datetime asc
            ) as rn_asc
        from
            appointment_deduped
    )
select
    1 as attribute_id,
    array_agg (
        distinct date_trunc (appointment_date, week (Monday))
    ) as appointment_weeks,
    count(distinct patient_id) as patient_count,
    count(distinct appointment_id) as appointment_count,
    count(
        distinct case
            when appointment_mode = 'Virtual' then appointment_id
        end
    ) as virual_appointment_count,
    count(
        distinct case
            when appointment_mode = 'In Person' then appointment_id
        end
    ) as in_person_appointment_count,
    count(
        distinct case
            when appointment_mode = 'Virtual' then appointment_id
        end
    ) / count(distinct appointment_id) as followup_appt_virtual_pct,
    count(
        distinct case
            when appointment_mode = 'In Person' then appointment_id
        end
    ) / count(distinct appointment_id) as followup_appt_in_person_pct
from
    appointment_staging
where
    rn_asc != 1
    and date(date_trunc (appointment_date, week (Monday))) >= date_sub (current_date, interval 9 week)
    and date(date_trunc (appointment_date, week (Monday))) != date(date_trunc (current_date, week (Monday)))
group by all
