--create or replace table capacity_plan.demand_final as
with
    actual_forecast_spine as (
        select
            c.create_week_start as reporting_week,
            c.run_type,
            c.dimension,
            c.dimension_value,
            c.referral_lower_bound_count,
            c.referral_count,
            c.referral_upper_bound_count,
            1 as attribute_id
        from
            `capacity_plan.referral_count_arima_model_staging` c
        union all
        select
            timestamp(date_trunc (creation_datetime, week (Monday))) as reporting_week,
            'Actuals' as run_type,
            'client' as dimension,
            client as dimension_value,
            max(null) as referral_lower_bound_count,
            count(distinct ID) as referral_count,
            max(null) as referral_upper_bound_count,
            1 as attribute_id
        from
            `clinical_reporting_pipeline.inbound_referrals`
        where
            client is not null
            and ID is not null
            and client = 'Conviva'
            and date(date_trunc (creation_datetime, week (Monday))) < date(date_trunc (current_date, week (Monday)))
        group by all
    )
select
    a.reporting_week,
    a.run_type,
    a.dimension,
    a.dimension_value,
    a.referral_lower_bound_count,
    a.referral_count,
    a.referral_upper_bound_count,
    engaged_pct_8.engaged_appt_scheduled_pct_last_8_cohorts_avg,
    a.referral_lower_bound_count * engaged_pct_8.engaged_appt_scheduled_pct_last_8_cohorts_avg as projected_engaged_referral_lower_bound_count,
    a.referral_count * engaged_pct_8.engaged_appt_scheduled_pct_last_8_cohorts_avg as projected_engaged_referral_lower_count,
    a.referral_upper_bound_count * engaged_pct_8.engaged_appt_scheduled_pct_last_8_cohorts_avg as projected_engaged_referral_upper_bound_count,
    ttf.time_to_first_appt_8_wk_avg
from
    actual_forecast_spine a
    left join capacity_plan.engaged_7d_pct_last_8_cohorts_avg engaged_pct_8 on a.attribute_id = engaged_pct_8.attribute_id
    and a.run_type = 'Forecast'
    left join capacity_plan.time_to_first_appt_8_wk_avg ttf on a.attribute_id = ttf.attribute_id
    and a.run_type = 'Forecast'
order by
    reporting_week desc,
    run_type desc
