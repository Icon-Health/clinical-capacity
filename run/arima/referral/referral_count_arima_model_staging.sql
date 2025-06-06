create
or replace table capacity_plan.referral_count_arima_model_final as
with
    staging as (
        select
            timestamp(date_trunc (creation_datetime, week (Monday))) as create_week_start,
            'Actuals' as run_type,
            'client' as dimension,
            client as dimension_value,
            count(distinct ID) as referral_count,
            null as referral_lower_bound_count,
            null as referral_upper_bound_count
        from
            `clinical_reporting_pipeline.inbound_referrals`
        where
            client is not null
            and ID is not null
            and client = 'Conviva'
        group by all
        union all
        select
            forecast_timestamp as create_week_start,
            'Forecast' as run_type,
            'client' as dimension,
            'Conviva' as dimension_value,
            round(forecast_value, 0) as referral_count,
            round(prediction_interval_lower_bound, 0) as referral_lower_bound_count,
            round(prediction_interval_upper_bound, 0) as referral_upper_bound_count
        from
            capacity_plan.referral_count_arima_model_forecast
    )
select
    *,
    current_timestamp as run_time
from
    staging
order by
    create_week_start desc
