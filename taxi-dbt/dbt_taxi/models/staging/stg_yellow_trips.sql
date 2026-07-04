with jan as (
    select * from read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet')
),
feb as (
    select * from read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet')
),
mar as (
    select * from read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet')
),
unioned as (
    select *, '2024-01' as source_month from jan
    union all
    select *, '2024-02' as source_month from feb
    union all
    select *, '2024-03' as source_month from mar
)
select
    cast(VendorID as integer) as vendor_id,
    cast(tpep_pickup_datetime as timestamp) as pickup_ts,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,
    cast(PULocationID as integer) as pu_location_id,
    cast(DOLocationID as integer) as do_location_id,
    cast(passenger_count as double) as passenger_count,
    cast(trip_distance as double) as trip_distance,
    cast(fare_amount as double) as fare_amount,
    cast(extra as double) as extra,
    cast(mta_tax as double) as mta_tax,
    cast(tip_amount as double) as tip_amount,
    cast(tolls_amount as double) as tolls_amount,
    cast(improvement_surcharge as double) as improvement_surcharge,
    cast(total_amount as double) as total_amount,
    cast(payment_type as integer) as payment_type,
    source_month
from unioned