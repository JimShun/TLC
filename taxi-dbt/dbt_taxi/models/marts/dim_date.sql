with dates as (
    select distinct pickup_date as date_day
    from {{ ref('silver_trips') }}
)
select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(day from date_day) as day,
    strftime(date_day, '%W') as week_of_year,
    case when extract(dow from date_day) in (0,6) then true else false end as is_weekend
from dates