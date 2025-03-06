with trips_data as (
    select * from {{ ref('fact_trips') }}
    where EXTRACT(YEAR FROM TIMESTAMP(pickup_datetime))  between 2019 and 2020
), trips_months as (
select 
    EXTRACT(YEAR FROM TIMESTAMP(pickup_datetime)) as year,
    EXTRACT(MONTH FROM TIMESTAMP(pickup_datetime)) as month,
    total_amount,
    service_type
    from trips_data
), trips_quarter as (select 
    *,
    {{ get_quarter('month') }} as quarter
    from trips_months
), trips_calculate as (
    select 
    year,quarter,service_type,
    sum(total_amount) as total_amount 
    from trips_quarter
    group by year,quarter,service_type
)
select g2.service_type,g2.quarter,
    g1.total_amount as t_2020, 
    g2.total_amount as t_2019,
    --(g1.total_amount - g2.total_amount) as total_amount,
    ROUND(
        ((g1.total_amount - g2.total_amount) / g2.total_amount) * 100, 2
    ) AS revenue_growth_percentage
from trips_calculate as g1
join trips_calculate as g2 on g1.quarter = g2.quarter and g1.service_type = g2.service_type
where g1.year = 2020 and g2.year = 2019
order by g2.service_type,g1.quarter

--26440852.61
--11480845.79