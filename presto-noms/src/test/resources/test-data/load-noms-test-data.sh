#!/usr/bin/env bash

DB_DIR=/tmp/presto-noms

import() {
    pk=$2
    types=$3
    db=$4
    ds=$5
    dbpath=${DB_DIR}/${db}
    dsspec=nbs:${dbpath}::${ds}
    mkdir -p ${dbpath}
    echo csv-import --lowercase --column-types="${types}" --dest-type="map:$pk" --meta primaryKey="$pk" ${ds}.csv ${dsspec}
    csv-import --lowercase --column-types="${types}" --dest-type=list ${ds}.csv ${dsspec}
    csv-import --lowercase --column-types="${types}" --dest-type="map:$pk" --meta primaryKey="$pk" ${ds}.csv ${dsspec}-map
}

rm -fr ${DB_DIR}/*

lineitem_types="Number,Number,Number,Number,Number,Number,Number,String,String,String,String,String,String,String,String"
import partkey $lineitem_types example lineitem

orders_types="Number,Number,String,Number,String,String,String,Number,String"
import orderkey $orders_types example orders
