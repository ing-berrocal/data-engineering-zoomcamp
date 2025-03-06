{{
    config(
        materialized='table'
    )
}}

with quarterly_revenue as (
    select 
        EXTRACT(YEAR FROM TIMESTAMP(pickup_datetime)) as year,
        EXTRACT(MONTH FROM TIMESTAMP(pickup_datetime)) as month,
        fare_amount,
        service_type
    from {{ ref('fact_trips') }}
    where fare_amount > 0 and trip_distance > 0 and payment_type_description in ('Cash','Credit card')
    --group by service_type, year, quarter, year_quarter
), f_calculo as (
    select 
        service_type,
        year,
        month,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(97)] AS p97,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(95)] AS p95,
        APPROX_QUANTILES(fare_amount, 100)[SAFE_OFFSET(90)] AS p90
        from quarterly_revenue
        where year = 2020 and month = 4
        GROUP BY service_type, year, month
)
select * from f_calculo