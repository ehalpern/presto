### Setup

Add these aliases to .nomsconfig  
```
[db.taxisamples]
	url = "http://presto.attic.io:8000"

[db.nyctaxidata]
	url = "http://presto.attic.io:8001"
```

Install the presto client
```
brew install presto
```

## Demo

Show either trips100m or trips397m

```
# show trips100m
noms show taxisamples::trips100m
```

```
# show trips397m
noms show nyctaxidata::trips397m
```

Start the presto client
```
presto --server presto.attic.io:8080 --catalog noms-thrift
```

Alternatively, you can run the client on presto.attic.io
```
ssh presto.attic.io
sudo su -l ec2-user
presto --server localhost:8080 --catalog noms-thrift
```

To show databases (at presto prompt)
```
show schemas;
```

Use either taxisamples (for trips100m) or nyctaxidata (for trips397m)

```
SELECT count(*) FROM taxisamples.trips100m;
-- Or 
SELECT count(*) FROM nyctaxidata.trips397m;
```

```
SELECT cab_type, count(*) FROM taxisamples.trips100m GROUP BY cab_type;
-- Or 
SELECT cab_type, count(*) FROM nyctaxidata.trips397m GROUP BY cab_type;
```

```
SELECT passenger_count, avg(total_amount) FROM taxisamples.trips100m GROUP BY passenger_count;
-- Or 
SELECT passenger_count, avg(total_amount) FROM nyctaxidata.trips397m GROUP BY passenger_count;
```
