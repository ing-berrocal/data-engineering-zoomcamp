/*
with rnk_90 as (
    select
        pickup_location_id,
        pickup_zone,
        dropoff_location_id,
        dropoff_zone,
        p90_trip_duration,
        RANK() OVER (PARTITION BY pickup_zone ORDER BY p90_trip_duration DESC) as rank
    from {{ ref('fct_fhv_monthly_zone_traveltime_p90') }}
    where year = 2019 and month = 11
    and pickup_zone in ('Newark Airport', 'SoHo', 'Yorkville East')
)
select pickup_zone, dropoff_zone, p90_trip_duration
from rnk_90
where rank=2
*/
select * from {{ ref('fct_fhv_monthly_zone_traveltime_p90') }}