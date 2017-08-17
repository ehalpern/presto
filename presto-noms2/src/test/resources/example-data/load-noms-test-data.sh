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
    #noms2 show nbs:/tmp/presto-noms2/${db}::${ds}
}

number_fields="text,value"
number_types="String,Number"
import $number_fields 0 $number_types "example" "numbers"

lineitem_fields="orderkey,partkey,suppkey,linenumber,quantity,discount,tax,returnflag,linestatus,shipdate,commitdate,receiptdate,shipinstruct,shipmode,comment"
lineitem_types="Number,Number,Number,Number,Number,Number,Number,String,String,String,String,String,String,String,String"
import $lineitem_fields 1 $lineitem_types "tpch" "lineitem"

orders_fields="orderkey,custkey,orderstatus,totalprice,orderdate,orderpriority,clerk,shippriority,comment"
orders_types="Number,Number,String,Number,String,String,String,Number,String"
import $orders_fields 0 $orders_types "tpch" "orders"
