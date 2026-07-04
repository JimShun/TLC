select *
from {{ ref('silver_trips') }}
where trip_distance < 0
   or trip_distance > 100