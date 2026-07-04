select
    cast(LocationID as integer) as location_id,
    Borough as borough,
    Zone as zone,
    service_zone
from read_csv_auto('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv', header=true)