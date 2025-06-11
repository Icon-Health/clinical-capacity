create
or replace model `capacity_plan.referral_count_arima_model` options (
    model_type = 'ARIMA_PLUS',
    time_series_timestamp_col = 'creation_date',
    time_series_data_col = 'referral_count',
    auto_arima = TRUE,
    data_frequency = 'daily',
    decompose_time_series = TRUE
) as
select
    timestamp(date(creation_datetime)) as creation_date,
    count(distinct ID) as referral_count
from
    `clinical_reporting_pipeline.inbound_referrals`
where
    client is not null
    and ID is not null
    and client = 'Conviva'
group by all
order by
    creation_date desc
