with base as (
    select
        vendor_id,
        pickup_ts,
        dropoff_ts,
        cast(pickup_ts as date) as pickup_date,
        extract(hour from pickup_ts) as pickup_hour,
        pu_location_id,
        do_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        payment_type,
        datediff('minute', pickup_ts, dropoff_ts) as trip_duration_min,
        source_month
    from {{ ref('stg_yellow_trips') }}
),
filtered as (
    select *
    from base
    where fare_amount > 0
      and trip_distance >= 0
      and dropoff_ts >= pickup_ts
      and datediff('minute', pickup_ts, dropoff_ts) between 0 and 300
),
deduped as (
    select *
    from (
        select
            *,
            row_number() over (
                partition by vendor_id, pickup_ts, dropoff_ts, pu_location_id, do_location_id, fare_amount, total_amount
                order by source_month desc
            ) as rn
        from filtered
    ) t
    where rn = 1
),
valid_fk as (
    select d.*
    from deduped d
    inner join {{ ref('stg_zones') }} z1 on d.pu_location_id = z1.location_id
    inner join {{ ref('stg_zones') }} z2 on d.do_location_id = z2.location_id
)
select
    vendor_id,
    pickup_ts,
    dropoff_ts,
    pickup_date,
    pickup_hour,
    pu_location_id,
    do_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type,
    trip_duration_min,
    source_month
from valid_fk