with base as (
    select
        strftime(pickup_date, '%Y-%m') as pickup_month,
        pu_zone_id,
        do_zone_id,
        count(*) as trip_count,
        sum(total_amount) as revenue
    from {{ ref('fact_trip') }}
    group by 1,2,3
),
ranked as (
    select
        *,
        row_number() over(partition by pickup_month order by trip_count desc, revenue desc) as rank_in_month
    from base
)
select
    pickup_month,
    pu_zone_id,
    do_zone_id,
    trip_count,
    round(revenue,2) as revenue,
    rank_in_month
from ranked
where rank_in_month <= 10
order by pickup_month, rank_in_month