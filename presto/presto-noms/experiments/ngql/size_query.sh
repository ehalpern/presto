#!/usr/bin/env bash
ds=$1

time curl http://localhost:8000/graphql/?ds=$ds -d 'query={
  root {
    value {
      passenger_count { size }
    }
  }
}'
