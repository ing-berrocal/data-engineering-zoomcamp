with fhv_table as (

    select * from {{ source('staging', 'fhv_table') }}

),
stg_fhv_table as (

    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid,
        dolocationid,
        sr_flag,
        affiliated_base_number

    from fhv_table

)

select * from stg_fhv_table
