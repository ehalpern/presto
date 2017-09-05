#!/usr/bin/env bash

DB_DIR=/tmp/presto-noms

import() {
    fields=$1
    pk=$2
    types=$3
    db=$4
    ds=$5
    dbpath=${DB_DIR}/${db}
    dsspec=nbs:${dbpath}::${ds}
    echo ${fields} > ${ds}-all.csv
    cat ${ds}-?.csv | sed 's/, /,/g' >> ${ds}-all.csv
    mkdir -p ${db_path}
    echo csv-import --column-types="${types}" --dest-type="map:$pk" --meta primaryKey="$pk" ${ds}-all.csv ${dsspec}
    csv-import --column-types="${types}" --dest-type="map:$pk" --meta primaryKey="$pk" ${ds}-all.csv ${dsspec}
    csv-import --column-types="${types}" --dest-type=list ${ds}-all.csv ${dsspec}-list
}

rm -fr ${DB_DIR}/*

test_fields="typestring,typebool,typedouble"
test_types="String,Bool,Number"
import $test_fields typestring $test_types test types

number_fields="text,value"
number_types="String,Number"
import $number_fields text $number_types test numbers

lineitem_fields="orderkey,partkey,suppkey,linenumber,quantity,discount,tax,returnflag,linestatus,shipdate,commitdate,receiptdate,shipinstruct,shipmode,comment"
lineitem_types="Number,Number,Number,Number,Number,Number,Number,String,String,String,String,String,String,String,String"
import $lineitem_fields partkey $lineitem_types example lineitem

orders_fields="orderkey,custkey,orderstatus,totalprice,orderdate,orderpriority,clerk,shippriority,comment"
orders_types="Number,Number,String,Number,String,String,String,Number,String"
import $orders_fields orderkey $orders_types example orders
