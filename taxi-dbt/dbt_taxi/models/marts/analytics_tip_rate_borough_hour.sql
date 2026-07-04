with joined as (
    select
        z.borough as pickup_borough,
        f.pickup_hour,
        f.fare_amount,
        f.tip_amount
    from {{ ref('fact_trip') }} f
    left join {{ ref('dim_zone') }} z
      on f.pu_zone_id = z.zone_id
)
select
    pickup_borough,
    pickup_hour,
    count(*) as trip_count,
    round(avg(case when fare_amount > 0 then (tip_amount / fare_amount) * 100 end), 2) as tip_rate_pct
from joined
group by 1,2
order by 1,2