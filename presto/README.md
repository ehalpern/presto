# Noms Connector for Presto

Provides a connector for querying noms bucketdb dataset using Presto.

This project originated as a fork of [prestodb/presto](https://github.com/prestodb/presto).  See the [Presto README](PRESTO-README.md) for presto details.  

## Setup
__Build Presto (details in [Presto README](PRESTO-README.md#building-presto)):__

   ``` 
   ./mvnw clean install               # with tests
   ```
 
   ```
    ./mvnw clean install -DskipTests   # without tests
   ```
__Configure IntelliJ__

- Download  [IntelliJ IDEA](http://www.jetbrains.com/idea/download) if needed
- Open the project in IntelliJ
- Open the File menu and select Project Structure
- In the SDKs section, ensure that a 1.8 JDK is selected (create one if none exist)
- In the Project section, ensure the language level is set to 8.0
- Presto comes with sample configuration that should work out-of-the-box for development. Use the following options to create a run configuration:
  - Main Class: `com.facebook.presto.server.PrestoServer`
  - VM Options: `-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties`
  - Working directory: `$MODULE_DIR$`
  - Use classpath of module: `presto-server`
- Configure code style as described in the [Developers](PRESTO-README.md##developers) section of the Presto README 
 
## Querying noms data

Start the noms server

    noms serve nbs:/tmp/presto-noms/test

Start the presto server in the IDE by running presto-server 

Start the presto CLI:

    presto-cli/target/presto-cli-*-executable.jar --server localhost:8080 --catalog noms

Verify that the server's running:

    SELECT * FROM system.runtime.nodes;

Verify that the noms **test** database is visible:

    SHOW SCHEMAS;

Checkout available tables (i.e. datasets):

    USE test;
    SHOW TABLES;

Checkout a table schema:

    DESCRIBE lineitem;

Query some data:

    SELECT * from lineitme;
    SELECT orderkey, quanity FROM lineitem WHERE quantity < 5;

