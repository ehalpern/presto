## NYC Taxi Data

This project provides the instructions and scripts for loading [NYC taxi data](http://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/) into bucketdb. This is a large, interesting dataset that's been used in a series of [benchmarks](http://tech.marksblogg.com/benchmarks.html) comparing many contemporary OLAP systems. 

The resulting bucketdb data is available in aws://bucketdb-manifests/bucketdb-tables/p/bucketdb.

Smaller data samples are available in aws://bucketdb-manifests/bucketdb-tables/p/smaller.


#### Step 1: Ingest and normalize 

Ingesting and normalizing NYC taxi data using [Todd Schneider's Project](https://github.com/toddwschneider/nyc-taxi-data)

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

#### Step 2: Export data to csv's

Join and export data as csv's as desribed in [A Billion Taxi Rides in Redshift](http://tech.marksblogg.com/billion-nyc-taxi-rides-redshift.html)

```
cd ~
./export.sh
```

#### Step 3: Import into bucketdb

TBD