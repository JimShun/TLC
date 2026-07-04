{{ config(
    materialized='incremental',
    unique_key='trip_sk'
) }}

with src as (
    select * from {{ ref('silver_trips') }}
    {% if is_incremental() %}
      where pickup_date >= (select coalesce(max(pickup_date), date '1900-01-01') from {{ this }})
    {% endif %}
),
final as (
    select
        md5(
          coalesce(cast(vendor_id as varchar), '') || '|' ||
          coalesce(cast(pickup_ts as varchar), '') || '|' ||
          coalesce(cast(dropoff_ts as varchar), '') || '|' ||
          coalesce(cast(pu_location_id as varchar), '') || '|' ||
          coalesce(cast(do_location_id as varchar), '') || '|' ||
          coalesce(cast(fare_amount as varchar), '') || '|' ||
          coalesce(cast(total_amount as varchar), '')
        ) as trip_sk,
        vendor_id,
        pickup_ts,
        dropoff_ts,
        pickup_date,
        pickup_hour,
        pu_location_id as pu_zone_id,
        do_location_id as do_zone_id,
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
    from src
)
select * from final