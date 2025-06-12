with
    staging as (
        select
            date_trunc (creation_datetime, week (Monday)) as creation_week,
            count(distinct rp.patient_id) as patient_count,
            'client' as dimension,
            rp.fields_Client as dimension_value,
            avg(t.time_to_first_appt) as time_to_first_appt_avg
        from
            {{ source('clinical_reporting_pipeline', 'registered_patients')}} rp
            left join  {{ source('clinical_reporting_pipeline', 'time_to_first_appointment')}} t on rp.patient_id = t.patient_id
        where
            t.time_to_first_appt is not null
            and rp.fields_Client = 'Conviva'
        group by all
        order by
            creation_week desc
    )
select
    dimension,
    dimension_value,
    1 as attribute_id,
    array_agg (distinct creation_week) as creation_weeks,
    sum(patient_count) as patient_count,
    avg(time_to_first_appt_avg) as time_to_first_appt_8_wk_avg
from
    staging
where
    date(creation_week) >= date_sub (current_date, interval 9 week)
    and date(creation_week) != date(date_trunc (current_date, week (Monday)))
group by all
