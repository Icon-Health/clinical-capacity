create
or replace table capacity_plan.actual_visits_scheduled as
select
    date_trunc (appointment_datetime, week (Monday)) as appointment_week,
    count(
        distinct (
            case
                when appointment_mode = 'In Person' then appointment_id
            end
        )
    ) as week_actual_in_person_visits_scheduled,
    count(
        distinct (
            case
                when appointment_mode = 'Virtual' then appointment_id
            end
        )
    ) as week_actual_virtual_visits_scheduled
from
    `clinical_reporting_pipeline.appointments`
where
    client = 'Conviva'
    and date(date_trunc (appointment_datetime, week (Monday))) > date_trunc (current_date, week (Monday))
    and appointment_status != 'cancelled'
group by all
