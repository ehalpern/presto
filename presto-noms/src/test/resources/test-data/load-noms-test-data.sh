#!/usr/bin/env bash

DB_DIR=/tmp/presto-noms

import() {
    pk=$1
    types=$2
    db=$3
    ds=$4
    dbpath=${DB_DIR}/${db}
    dsspec=nbs:${dbpath}::${ds}
    mkdir -p ${dbpath}
    (set -x; csv-import --lowercase --column-types="${types}" --dest-type=list ${ds}.csv ${dsspec})

    if [ "$pk" != "" ]
    then
        (set -x; csv-import --lowercase --column-types="${types}" --dest-type="map:$pk" --meta primaryKey="$pk" ${ds}.csv ${dsspec}-map)
    fi
}

rm -fr ${DB_DIR}/*

lineitem_types="Number,Number,Number,Number,Number,Number,Number,String,String,String,String,String,String,String,String"
import partkey $lineitem_types example lineitem

orders_types="Number,Number,String,Number,String,String,String,Number,String"
import orderkey $orders_types example orders

trips_types="String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String,String"
import "" $trips_types test trips10k
import "" $trips_types test trips1m
