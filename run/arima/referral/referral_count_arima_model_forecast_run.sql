create
or replace table capacity_plan.referral_count_arima_model_forecast_run as
SELECT
    *,
    current_date as run_date
FROM
    ML.FORECAST (
        MODEL `capacity_plan.referral_count_arima_model`,
        -- 16_weeks_out
        STRUCT (16 AS horizon, 0.8 AS confidence_level)
    )
order by
    forecast_timestamp asc
