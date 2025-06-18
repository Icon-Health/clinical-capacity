with appointment_actual_staging as (
    select
        date(appointment_datetime) as appointment_date,
        count(distinct provider_id) as provider_count,
        coalesce(
            sum(case when appointment_mode = 'In Person' then duration_minutes end) / 60,
            0
        ) as in_person_appointment_hours,
        coalesce(
            sum(case when appointment_mode = 'Virtual' then duration_minutes end) / 60,
            0
        ) as virtual_appointment_hours,
        (
            coalesce(sum(case when appointment_mode = 'In Person' then duration_minutes end) / 60, 0) +
            coalesce(sum(case when appointment_mode = 'Virtual' then duration_minutes end) / 60, 0)
        ) as appointment_hours
    from {{source('clinical_reporting_pipeline','appointments')}}
    where client in ('Conviva','Primus')
          and
          cancelledat is null
          or (safe_cast(cancelledat as date) = cast(appointment_datetime as date))
    group by 1
),

week_aggs as (
    select
        date_trunc(appointment_date, week(Monday)) as appointment_week,
        max(provider_count) as provider_count,
        sum(in_person_appointment_hours) as in_person_appointment_hours,
        sum(virtual_appointment_hours) as virtual_appointment_hours,
        sum(appointment_hours) as appointment_hours,
        sum(appointment_hours) / max(provider_count) as avg_appointment_hours_per_week_per_provider
    from appointment_actual_staging
    group by 1
)

,aggs_final as (
select
1 as attribute_id,
array_agg(distinct appointment_week) as appointment_weeks,
array_agg(distinct provider_count) as provider_counts,
array_agg(distinct in_person_appointment_hours) as in_person_appointment_hours,
array_agg(distinct virtual_appointment_hours) as virtual_appointment_hours,
array_agg(distinct appointment_hours) as appointment_hours,
array_agg(distinct avg_appointment_hours_per_week_per_provider) as avg_appointment_hours_per_week_per_providers,
avg(avg_appointment_hours_per_week_per_provider) as avg_appointment_hours_per_week_per_provider,
from week_aggs
where appointment_week < date_trunc(current_date(), week(Monday))
      and appointment_week > date_sub(current_date(), interval 7 week)
group by all
)

select
*,
30 as ideal_appointment_hours_per_week_per_provider,
round(provider_counts[0] * avg_appointment_hours_per_week_per_provider,0) as supply_hours_available_actuals,
round(provider_counts[0] * 30,0) as supply_hours_available_ideal,

from aggs_final
