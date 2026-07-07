select *
from {{ ref('stg_yellow_trips') }}
where datediff('minute', pickup_ts, dropoff_ts) < 0
   or datediff('minute', pickup_ts, dropoff_ts) > 300