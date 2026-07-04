select *
from {{ ref('silver_trips') }}
where trip_duration_min < 0
   or trip_duration_min > 300