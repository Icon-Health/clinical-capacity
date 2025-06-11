create
or replace table capacity_plan.referral_count_arima_model_staging as
with
    staging as (
        select
            forecast_timestamp as creation_date,
            'Forecast' as run_type,
            'client' as dimension,
            'Conviva' as dimension_value,
            forecast_value as referral_count,
            prediction_interval_lower_bound as referral_lower_bound_count,
            prediction_interval_upper_bound as referral_upper_bound_count
        from
            capacity_plan.referral_count_arima_model_forecast_run
    )
select
    *,
    current_timestamp as run_time
from
    staging
order by
    creation_date desc
