# Noms Connector for Presto

This is a fork of [prestodb/presto](https://github.com/prestodb/presto) that adds the [presto-noms](presto-noms) connector. See the [Presto README](PRESTO-README.md) for presto details  

## Setup
Clone this repo:

    git clone git@github.com:ehalpern/presto.git

Build Presto (details in [Presto README](PRESTO-README.md#building-presto)):

   ``` 
   ./mvnw clean install               # with tests
   ```
 
   ```
    ./mvnw clean install -DskipTests   # without tests
   ```

Download  [IntelliJ IDEA](http://www.jetbrains.com/idea/download) if you don't have it

Follow Presto README instructions for [IDE setup](PRESTO-README.md##running-presto-in-your-ide)
 
## Querying noms data

Start the noms server

    noms serve nbs:/tmp/presto-noms/test

Start the presto server in the IDE by running presto-main 

Start the presto CLI:

    presto-cli/target/presto-cli-*-executable.jar --server localhost:8080 --catalog noms

Verify that the server's running:

    SELECT * FROM system.runtime.nodes;

Verify that the noms **example** database is visible:

    SHOW SCHEMAS;

Checkout available tables (i.e. datasets):

    USE example;
    SHOW TABLES;

Checkout a table schema:

    DESCRIBE lineitem;

Query some data:

    SELECT * from lineitme;
    SELECT orderkey, quanity FROM lineitem WHERE quantity < 5;

