select
    days_since_referral_created,
    1 as attribute_id,
    array_agg (distinct creation_week) as creation_weeks,
    count(distinct patient_id) as patient_count,
    sum(engaged_appt_scheduled) / sum(referral_value) as engaged_appt_scheduled_pct_last_8_cohorts_avg
from
     {{source('clinical_reporting_pipeline','registered_patients_cohorts_7d')}}
where
    {{ var('dimension') }} = '{{ var('dimension_value') }}'
    and creation_week >= date_sub (current_date, interval 8 week)
group by all
having
    days_since_referral_created = 7
