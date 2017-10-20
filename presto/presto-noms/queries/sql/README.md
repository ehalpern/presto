```
SELECT cab_type,
       count(*)
FROM trips
GROUP BY cab_type;
```

```
SELECT passenger_count,
       avg(total_amount)
FROM trips
GROUP BY passenger_count;
```

```
SELECT passenger_count,
       regexp_extract(pickup_datetime, '\d+-\d+-\d+') trip_year,
       count(*)
FROM trips
GROUP BY passenger_count,
         regexp_extract(pickup_datetime, '\d+-\d+-\d+');
```

```
SELECT passenger_count,
       regexp_extract(pickup_datetime, '\d+-\d+-\d+') trip_year,
       round(trip_distance),
       count(*) trips
FROM trips
GROUP BY passenger_count,
         regexp_extract(pickup_datetime, '\d+-\d+-\d+'),
         round(trip_distance)
ORDER BY trip_year,
         trips desc;
```

