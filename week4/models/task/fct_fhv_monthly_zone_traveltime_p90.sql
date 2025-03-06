{{
    config(
        materialized='table'
    )
}}

with trips_data as (
    select 
    EXTRACT(YEAR FROM TIMESTAMP(pickup_datetime)) as year,
    EXTRACT(MONTH FROM TIMESTAMP(pickup_datetime)) as month,
    * from {{ ref('fact_trips') }}
), trips_months as (
select 
    *,
    TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) diff_seconds
    from trips_data
),p90_duration as (
    select
        year,
        month,
        pickup_locationid,
        dropoff_locationid,
        pickup_zone,
        dropoff_zone
        ,
        PERCENTILE_CONT(diff_seconds, 0.90) OVER (
            PARTITION BY year, month, pickup_locationid, dropoff_locationid
        ) as p90_tduration
    from trips_months
)
select distinct year, month, pickup_locationid, dropoff_locationid, pickup_zone, dropoff_zone, p90_tduration
from p90_duration