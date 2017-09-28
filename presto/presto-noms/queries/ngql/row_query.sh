#!/usr/bin/env bash
ds=$1
time curl http://localhost:8000/graphql/?ds=${ds} -d 'query={
  root {
    value {
      passenger_count       { values }
      pulocationid          { values }
      extra                 { values }
      mta_tax               { values }
      tpep_dropoff_datetime { values }
      trip_distance         { values }
      improvement_surcharge { values }
      tip_amount 		    { values }
      total_amount 		 { values }
      dolocationid 		 { values }
      vendorid 			 { values }
      payment_type 		 { values }
      tpep_pickup_datetime 	 { values }
      ratecodeid 		 { values }
      tolls_amount 		 { values }
      fare_amount 		 { values }
      store_and_fwd_flag 	 { values }
    }
  }
}'
