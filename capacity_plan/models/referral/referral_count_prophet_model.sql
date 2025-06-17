select
    timestamp(p.ds) as ds,
    if(p.yhat < 0, 0, p.yhat) as yhat,
    if(p.yhat_lower < 0, 0, p.yhat_lower) as yhat_lower,
    if(p.yhat_upper < 0, 0, p.yhat_upper) as yhat_upper,
    c.day_name,
    p.run_date
from
    {{ source('capacity_plan','prophet_run')}} p
left join {{ source('analytics_manual','calendar')}} c
     on date(date(ds)) = c.calendar_date
where
    date(p.ds) > (
        select
            max(date(creation_datetime))
        from
            {{source('clinical_reporting_pipeline','inbound_referrals')}}
        where
            client is not null
            and ID is not null
            and {{ var('dimension') }} = '{{ var('dimension_value') }}'
    )

--and day_name not in ('Saturday','Sunday')
