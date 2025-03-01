with green_table as (
    select *,row_number() over(partition by vendorid, lpep_pickup_datetime) as rn
    from {{ source('staging', 'green_table') }}
    where vendorid is not null 

),

stg_green_table as (

    select
        vendorid,
        lpep_pickup_datetime,
        lpep_dropoff_datetime,
        store_and_fwd_flag,
        ratecodeid,
        pulocationid,
        dolocationid,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_description,
        trip_type,
        congestion_surcharge

    from green_table

)

select * from stg_green_table
where rn = 1

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
/*{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}*/