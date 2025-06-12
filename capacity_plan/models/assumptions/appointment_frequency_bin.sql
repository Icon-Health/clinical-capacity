with
    staging as (
        select
            patient_id,
            count(distinct appointment_id) as appointment_count
        from
            {{source('clinical_reporting_pipeline','appointments')}}

        where
            client = 'Conviva'
        group by
            1
    ),
    pivot_staging as (
        select
            case
                when appointment_count >= 2 then '2+'
                else '1'
            end as appointment_count_bin,
            1 as attribute_id,
            count(distinct patient_id) as patient_count
        from
            staging
        group by all
        order by
            1 asc
    ),
    pivot_final as (
        select
            attribute_id,
            max(
                if (appointment_count_bin = '1', patient_count, null)
            ) as one_appointment_patients,
            max(
                if (appointment_count_bin = '2+', patient_count, null)
            ) as two_plus_appointment_patients
        from
            pivot_staging
        group by
            1
    )
select
    attribute_id,
    one_appointment_patients,
    two_plus_appointment_patients,
    one_appointment_patients / (
        one_appointment_patients + two_plus_appointment_patients
    ) as patients_with_one_appointment_pct,
    two_plus_appointment_patients / (
        one_appointment_patients + two_plus_appointment_patients
    ) as patients_with_two_plus_appointment_pct
from
    pivot_final
