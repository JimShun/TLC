select
    pickup_date,
    count(*) as trip_count,
    round(sum(total_amount), 2) as revenue,
    round(avg(case when trip_distance > 0 then fare_amount / trip_distance end), 4) as avg_fare_per_mile
from {{ ref('fact_trip') }}
group by 1
order by 1