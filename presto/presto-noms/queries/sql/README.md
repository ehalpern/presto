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
       year(pickup_datetime),
       count(*)
FROM trips
GROUP BY passenger_count,
         year(pickup_datetime);
```

```
SELECT passenger_count,
       year(pickup_datetime) trip_year,
       round(trip_distance),
       count(*) trips
FROM trips
GROUP BY passenger_count,
         year(pickup_datetime),
         round(trip_distance)
ORDER BY trip_year,
         trips desc;
```

