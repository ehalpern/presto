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

Execute noms-import.sh to import the data from the csv files into a Noms database.
```
noms-import --dir ./export/trips <dsspec>
```
This will import the first 100 million rows into the noms database. You can add the arguments: "--size 1000", to import all the csv files (about ~1G rows). If you need to restart the script for some reason and you don't want to start importing from scratch, you can use "--skip <n>" to indicate that the first n rows should not be imported. This can be combined with "--append true" so that the existing dataset is appended to, rather than overwritten.
```
