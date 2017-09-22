- Implement describe for: List<Struct<Number, String, Bool>>
- Implement select for: List<Struct<Number, String, Bool>>
- Struct<T> -> Row<Struct<T>>
- List<T> -> Row<T>
- Map<K,V> -> Row<V> (with K as primary key?)
- Set<T> -> Row<T>


#### Predicate Pushdown


#### Work distribution 

Using hive connector as an example:

Consider the following table:

```
CREATE TABLE mytable ( 
         name string,
         city string,
         employee_id int ) 
PARTITIONED BY (year STRING, month STRING, day STRING) 
CLUSTERED BY (employee_id) INTO 256 BUCKETS
```

PARTITIONED BY 
  - defines a natural key used to partition storage
  - in this case, we're using a compound key to represent a date
  - in hive, the partition key is represented as a path in the filesystem as in:
  
```    
    /warehouse/mytable/y=2017/m=09/d=02
    /warehouse/mytable/y=2015/m=09/d=03
```
  - If a query contains constraints on these columns, the connector 
    can avoid reading paritions. Additionally, each parition can 
    be safely read by a separate worker
    
CLUSTERED BY
  - defines a column to hash into a fixed number of buckets
  - each bucket can be read/written by a separate worker

See HiveMetadata.getTableLayouts() to see how the hive connector reports partitioning
(and clustering) back to presto

- Given columns and query constraint, builds a list of partitions to be read 
  to satisfy query
- If the query constrains the partition key, it will only return a subset of 
  partitions
- If the query constrains the clustering key, each partition will indicate the 
  subset of buckets to read
- Splits the constraints into enforced and unenforced, where enforced are those
  refering to the partioning key comluns, and unenforced are the rest

Useful:

http://prestodb.rocks/internals/the-fundamentals-data-distribution/

Target queries: 


Schema:
```angular2html

``` 

```
SELECT cab_type, count(*)
FROM trips_orc
GROUP BY cab_type;
```

```
SELECT passenger_count,
       avg(total_amount)
FROM trips_orc
GROUP BY passenger_count;
```

```
SELECT passenger_count,
       year(pickup_datetime),
       count(*)
FROM trips_orc
GROUP BY passenger_count,
         year(pickup_datetime);
```

```
SELECT passenger_count,
       year(pickup_datetime) trip_year,
       round(trip_distance),
       count(*) trips
FROM trips_orc
GROUP BY passenger_count,
         year(pickup_datetime),
         round(trip_distance)
ORDER BY trip_year,
         trips desc;
```


Analyzing 1.1 Billion NYC Taxi and Uber Trips, with a Vengeance

Article: http://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/

Github: https://github.com/toddwschneider/nyc-taxi-data

Exporting data: http://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html


BigQuery and Dataproc shine in independent big data platform comparison

Article: https://cloud.google.com/blog/big-data/2016/05/bigquery-and-dataproc-shine-in-independent-big-data-platform-comparison


## Installing data

Use https://gist.github.com/whyvez/8d19096712ea44ba66b0 to install Postgres and PostGIS

```
psql -d nyc-taxi-data -c "COPY (
   SELECT trips.id,
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
        ON drop_off.gid = trips.dropoff_nyct2010_gid
) TO STDOUT WITH CSV" | split -l 20000000 --filter="gzip > ~/nyc-taxi-data/trips/trips_\$FILE.csv.gz" 



PROGRAM
   'split -l 20000000 --filter="gzip > ~/nyc-taxi-data/trips/trips_\$FILE.csv.gz"'
         WITH CSV;
```
