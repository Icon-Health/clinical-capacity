with
    staging as (
        select
            ds as creation_date,
            'Forecast' as run_type,
            '{{ var('dimension') }}'  as dimension,
            --'{{ var('dimension_value') }}' as dimension_value,
            'Conviva, Primus' as dimension_value,
            yhat as referral_count,
            yhat_lower as referral_lower_bound_count,
            yhat_upper as referral_upper_bound_count
        from {{ref('referral_count_prophet_model')}}
    )
select
    *,
    current_timestamp as run_time
from
    staging
order by
    creation_date desc
