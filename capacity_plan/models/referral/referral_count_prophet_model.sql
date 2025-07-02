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
            and client in ('Conviva','Primus')
            --and ({{ var('dimension') }} = '{{ var('dimension_value') }}'
            --     or {{ var('dimension') }} = '{{ var('dimension_value2') }}')
            --and date_trunc(ds,week(Monday)) != date_trunc(current_date,week(Monday))
    )
    and date_trunc(date(p.ds),week(MONDAY)) >= date_trunc(date(current_date),week(MONDAY))

--and day_name not in ('Saturday','Sunday')
