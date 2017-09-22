Install latest jdk

```
brew tap caskroom/cask # if not done previously
brew cask install java # install latest
brew cask info java    # verify version
/usr/libexec/java_home # verify version another way
```

Build disribution

```
./mvnw clean install -DskipTests
```

Setup IDE to run server. See https://github.com/prestodb/presto/blob/master/README.md#overview.

Run cli:

```
presto-cli/target/presto-cli-*-executable.jar
```

Verify:

```
SELECT * FROM system.runtime.nodes;
```

Setup mysql:

```
brew install mysql # if needed
```

Docs assume account root/root

Create and populate db:

```
mysql> create database tutorials;
create table author(auth_id int not null, auth_name varchar(50),topic varchar(100));
insert into author values(1,'Doug Cutting','Hadoop');
insert into author values(2,'James Gosling','java');
insert into author values(3,'Dennis Ritchie','C');
```

Verify:
```
mysql> select * from author
```

View using presto:

Ensure presto-main/etc/catalog/mysql.properties is this:
```
connector.name=mysql
connection-url=jdbc:mysql://localhost:3306
connection-user=root
connection-password=root
```

Start presto using ide or cli:
```

```

Start cli:
```
$ presto-cli/target/presto-cli-*-executable.jar --server localhost:8080 --catalog mysql --schema tutorials

presto:tutorials> show schemas from mysql;
presto:tutorials> show schemas from mysql;
presto:tutorials> show tables from mysql.tutorials;
presto:tutorials> show columns from mysql.tutorials.author
presto:tutorials> select * from mysql.tutorials.author;
```


Had issues with presto-verifier tutorial: https://www.tutorialspoint.com/apache_presto/apache_presto_quick_guide.htm.

```
SKIPPED: Invalid JDBC URL: jdbc:presto://localhost:8080
```

