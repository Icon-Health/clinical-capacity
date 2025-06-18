with
    appointment_actual_staging as (
        select
            date(appointment_datetime) as appointment_date,
            coalesce(
                sum(
                    case
                        when appointment_mode = 'In Person' then duration_minutes
                    end
                ) / 60,
                0
            ) as in_person_appointment_hours,
            coalesce(
                sum(
                    case
                        when appointment_mode = 'Virtual' then duration_minutes
                    end
                ) / 60,
                0
            ) as virtual_appointment_hours,
            (
                coalesce(
                    sum(
                        case
                            when appointment_mode = 'In Person' then duration_minutes
                        end
                    ) / 60,
                    0
                ) + coalesce(
                    sum(
                        case
                            when appointment_mode = 'Virtual' then duration_minutes
                        end
                    ) / 60,
                    0
                )
            ) as appointment_hours
        from
            {{source('clinical_reporting_pipeline','appointments')}}
        where
            client in ('Conviva','Primus')
        group by
            1
    )
select
    date_trunc (appointment_date, week (Monday)) as appointment_week,
    sum(in_person_appointment_hours) as in_person_appointment_hours_actuals,
    sum(virtual_appointment_hours) as virtual_appointment_hours_acual,
    sum(appointment_hours) as appointment_hours_actuals
from
    appointment_actual_staging
group by
    1
order by
    appointment_week desc
