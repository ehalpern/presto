#!/usr/bin/env bash

output=~/export/trips

query="SELECT trips.id,
       trips.vendor_id,
       trips.pickup_datetime,
       trips.dropoff_datetime,
       trips.store_and_fwd_flag,
       trips.rate_code_id,
       trips.pickup_longitude,
       trips.pickup_latitude,
       trips.dropoff_longitude,
       trips.dropoff_latitude,
       trips.passenger_count,
       trips.trip_distance,
       trips.fare_amount,
       trips.extra,
       trips.mta_tax,
       trips.tip_amount,
       trips.tolls_amount,
       trips.ehail_fee,
       trips.improvement_surcharge,
       trips.total_amount,
       trips.payment_type,
       trips.trip_type,
       trips.pickup,
       trips.dropoff,

       cab_types.type cab_type,

       weather.precipitation rain,
       weather.snow_depth,
       weather.snowfall,
       weather.max_temperature max_temp,
       weather.min_temperature min_temp,
       weather.average_wind_speed wind,

       pick_up.gid pickup_nyct2010_gid,
       pick_up.ctlabel pickup_ctlabel,
       pick_up.borocode pickup_borocode,
       pick_up.boroname pickup_boroname,
       pick_up.ct2010 pickup_ct2010,
       pick_up.boroct2010 pickup_boroct2010,
       pick_up.cdeligibil pickup_cdeligibil,
       pick_up.ntacode pickup_ntacode,
       pick_up.ntaname pickup_ntaname,
       pick_up.puma pickup_puma,

       drop_off.gid dropoff_nyct2010_gid,
       drop_off.ctlabel dropoff_ctlabel,
       drop_off.borocode dropoff_borocode,
       drop_off.boroname dropoff_boroname,
       drop_off.ct2010 dropoff_ct2010,
       drop_off.boroct2010 dropoff_boroct2010,
       drop_off.cdeligibil dropoff_cdeligibil,
       drop_off.ntacode dropoff_ntacode,
       drop_off.ntaname dropoff_ntaname,
       drop_off.puma dropoff_puma
    FROM trips
    LEFT JOIN cab_types
        ON trips.cab_type_id = cab_types.id
    LEFT JOIN central_park_weather_observations weather
        ON weather.date = trips.pickup_datetime::date
    LEFT JOIN nyct2010 pick_up
        ON pick_up.gid = trips.pickup_nyct2010_gid
    LEFT JOIN nyct2010 drop_off
        ON drop_off.gid = trips.dropoff_nyct2010_gid"

mkdir -p ${output}

# Export data in chunks
psql -d nyc-taxi-data -c "COPY (
   ${query}
) TO STDOUT WITH CSV" | split -l 20000000 --filter="gzip > ${output}/trips_\$FILE.csv.gz"

# Export header
psql -d nyc-taxi-data -c "COPY (
   ${query} LIMIT 1
) TO STDOUT WITH CSV HEADER" | head -1 >  ${output}/trips.header.csv

# Export types which are needed to specify --conlumn-types to csv-import
# TODO: It'd be cool if csv-import could instead infer the types based on the first rows.
echo "Number,String,String,String,String,String,Number,Number,Number,Number,Number,Number,Number,Number,Number,Number,Number,Number,Number,Number,String,String,String,String,String,Number,Number,Number,Number,Number,Number,Number,Number,Number,String,Number,Number,String,String,String,Number,Number,Number,Number,String,Number,Number,String,String,String,Number" > ${output}/trips.types.csv
