create
or replace model `capacity_plan.referral_count_arima_model` options (
    model_type = 'ARIMA_PLUS',
    time_series_timestamp_col = 'create_week_start',
    time_series_data_col = 'referral_count',
    auto_arima = TRUE,
    data_frequency = 'weekly',
    decompose_time_series = TRUE
) as
select
    timestamp(date_trunc (creation_datetime, week (Monday))) as create_week_start,
    count(distinct ID) as referral_count
from
    `clinical_reporting_pipeline.inbound_referrals`
where
    client is not null
    and ID is not null
    and client = 'Conviva'
group by all
order by
    create_week_start desc
