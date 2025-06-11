create
or replace table capacity_plan.time_to_followup_appt_8_wk_avg as
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
            appointment_datetime,
            date(appointment_datetime) as appointment_date,
            row_number() over (
                partition by
                    patient_id
                order by
                    appointment_datetime asc
            ) as rn_asc,
            lag (date(appointment_datetime)) over (
                partition by
                    patient_id
                order by
                    appointment_datetime desc
            ) as next_appointment_date
        from
            appointment_deduped
    ),
    historical_logic as (
        select
            *
        except
        (appointment_datetime),
        date_diff (next_appointment_date, appointment_date, day) as days_to_next_appointment,
        count(*) over (
            partition by
                patient_id
        ) as historical_appointment_count
        from
            appointment_staging
    )
select
    1 as attribute_id,
    array_agg (
        distinct date_trunc (appointment_date, week (Monday))
    ) as appointment_weeks,
    count(distinct patient_id) as patient_count,
    cast(avg(days_to_next_appointment) as int64) as time_to_followup_appt_8_wk_avg
from
    historical_logic
where
    historical_appointment_count > 1
    and next_appointment_date is not null
    and date(date_trunc (appointment_date, week (Monday))) >= date_sub (current_date, interval 9 week)
    and date(date_trunc (appointment_date, week (Monday))) != date(date_trunc (current_date, week (Monday)))
group by all
