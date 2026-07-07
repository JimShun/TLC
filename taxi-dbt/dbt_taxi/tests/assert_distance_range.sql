select *
from {{ ref('stg_yellow_trips') }}
where trip_distance < 0
   or trip_distance > 100