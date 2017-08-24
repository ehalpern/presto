#!/usr/bin/env bash


import() {
    fields=$1
    pk=$2
    types=$3
    db=$4
    ds=$5
    echo ${fields} > ${ds}-all.csv
    cat ${ds}-?.csv | sed 's/, /,/g' >> ${ds}-all.csv
    echo csv-import --column-types="${types}" --dest-type=map:$pk ${ds}-all.csv nbs:/tmp/presto-noms/${db}::${ds}
    csv-import --column-types="${types}" --dest-type=map:$pk ${ds}-all.csv nbs:/tmp/presto-noms/${db}::${ds}
    csv-import --column-types="${types}" --dest-type=list ${ds}-all.csv nbs:/tmp/presto-noms/${db}::${ds}-list
    #noms2 show nbs:/tmp/presto-noms2/${db}::${ds}
}

test_fields="typestring,typebool,typedouble"
test_types="String,Bool,Number"
import $test_fields 0 $test_types "test" "types"

number_fields="text,value"
number_types="String,Number"
import $number_fields 0 $number_types "test" "numbers"

lineitem_fields="orderkey,partkey,suppkey,linenumber,quantity,discount,tax,returnflag,linestatus,shipdate,commitdate,receiptdate,shipinstruct,shipmode,comment"
lineitem_types="Number,Number,Number,Number,Number,Number,Number,String,String,String,String,String,String,String,String"
import $lineitem_fields 1 $lineitem_types "example" "lineitem"

orders_fields="orderkey,custkey,orderstatus,totalprice,orderdate,orderpriority,clerk,shippriority,comment"
orders_types="Number,Number,String,Number,String,String,String,Number,String"
import $orders_fields 0 $orders_types "example" "orders"
