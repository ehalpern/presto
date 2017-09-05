- Implement describe for: List<Struct<Number, String, Bool>>
- Implement select for: List<Struct<Number, String, Bool>>
- Struct<T> -> Row<Struct<T>>
- List<T> -> Row<T>
- Map<K,V> -> Row<V> (with K as primary key?)
- Set<T> -> Row<T>


#### Predicate Pushdown


#### Work distribution 

Using hive connector as an example:

Consider the following table:

```
CREATE TABLE mytable ( 
         name string,
         city string,
         employee_id int ) 
PARTITIONED BY (year STRING, month STRING, day STRING) 
CLUSTERED BY (employee_id) INTO 256 BUCKETS
```

PARTITIONED BY 
  - defines a natural key used to partition storage
  - in this case, we're using a compound key to represent a date
  - in hive, the partition key is represented as a path in the filesystem as in:
  
```    
    /warehouse/mytable/y=2017/m=09/d=02
    /warehouse/mytable/y=2015/m=09/d=03
```
  - If a query contains constraints on these columns, the connector 
    can avoid reading paritions. Additionally, each parition can 
    be safely read by a separate worker
    
CLUSTERED BY
  - defines a column to hash into a fixed number of buckets
  - each bucket can be read/written by a separate worker

See HiveMetadata.getTableLayouts() to see how the hive connector reports partitioning
(and clustering) back to presto

- Given columns and query constraint, builds a list of partitions to be read 
  to satisfy query
- If the query constrains the partition key, it will only return a subset of 
  partitions
- If the query constrains the clustering key, each partition will indicate the 
  subset of buckets to read
- Splits the constraints into enforced and unenforced, where enforced are those
  refering to the partioning key comluns, and unenforced are the rest

Useful:

http://prestodb.rocks/internals/the-fundamentals-data-distribution/