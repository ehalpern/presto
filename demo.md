### Setup

Add these aliases to .nomsconfig  
```
[db.taxisamples]
	url = "http://presto.attic.io:8080"

[db.nyctaxidata]
	url = "http://presto.attic.io:8081"
```

Install the presto client
```
brew install presto
```

## Demo

Show either trips100m or trips1g

```
# show trips100m
noms show taxisamples::trips100m
```

```
# show trips1g
noms show nyctaxisamples::trips1g
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
show databases
```

Use either taxisamples (for trips100m) or nyctaxidata (for trips1g)
```
-- For trips100m
use taxisamples; 

-- For trips1g
use nyctaxidata;
```

```
SELECT count(*) FROM trips100m;
```

```
SELECT cab_type, count(*) FROM trip100m GROUP BY cab_type;
```

```
SELECT passenger_count, avg(total_amount) FROM trips100m GROUP BY passenger_count;
```
