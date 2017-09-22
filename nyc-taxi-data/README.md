## NYC Taxi Data

- Ingest and normalize NYC taxi data using [Todd Schneider's Project](https://github.com/toddwschneider/nyc-taxi-data)
- Join and export data as csv's as desribed in [A Billion Taxi Rides in Redshift](http://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html)

## Steps

Clone repo
```
git clone https://github.com/toddwschneider/nyc-taxi-data.git
cd nyc-taxi-data

```

Download and ingest public nyc taxi data
```
./download_raw_data.sh
./initialize_database.sh
./import_trip_data.sh
```

Download and ingest Limo data and FiveThirtyEight's Uber data 
```
./download_raw_uber_data.sh 
./import_uber_trip_data.sh 
./import_fhv_trip_data.sh
```

Export data to csv's
```
cd ~
./export.sh
```
