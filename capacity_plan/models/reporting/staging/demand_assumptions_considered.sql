with
    actual_forecast_spine as (
        select
            c.creation_date as reporting_date,
            c.run_type,
            c.dimension,
            c.dimension_value,
            c.referral_lower_bound_count,
            c.referral_count,
            c.referral_upper_bound_count,
            1 as attribute_id
        from
            {{ref('referral_count_model_staging')}} c
    )
select
    a.reporting_date,
    a.run_type,
    a.attribute_id,
    a.dimension,
    a.dimension_value,
    a.referral_lower_bound_count,
    a.referral_count,
    a.referral_upper_bound_count,
    engaged_pct_8.engaged_appt_scheduled_pct_last_8_cohorts_avg,
    a.referral_lower_bound_count * engaged_pct_8.engaged_appt_scheduled_pct_last_8_cohorts_avg as projected_engaged_referral_lower_bound_count,
    a.referral_count * engaged_pct_8.engaged_appt_scheduled_pct_last_8_cohorts_avg as projected_engaged_referral_count,
    a.referral_upper_bound_count * engaged_pct_8.engaged_appt_scheduled_pct_last_8_cohorts_avg as projected_engaged_referral_upper_bound_count,
    cast(round(time_to_first_appt_8_wk_avg, 0) as int64) as time_to_first_appt_8_wk_avg
from
    actual_forecast_spine a
    left join capacity_plan.engaged_7d_pct_last_8_cohorts_avg engaged_pct_8 on a.attribute_id = engaged_pct_8.attribute_id
    left join capacity_plan.time_to_first_appt_8_wk_avg ttf on a.attribute_id = ttf.attribute_id
order by
    reporting_date desc,
    run_type desc
