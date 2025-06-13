select
    timestamp(ds) as ds,
    yhat,
    yhat_lower,
    yhat_upper,
    run_date
from
    {{ source('capacity_plan','prophet_run')}}
where
    date(ds) > (
        select
            max(date(creation_datetime))
        from
            {{source('clinical_reporting_pipeline','inbound_referrals')}}
        where
            client is not null
            and ID is not null
            and {{ var('dimension') }} = '{{ var('dimension_value') }}'
    )
