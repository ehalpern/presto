#!/usr/bin/env bash

# Args with default values
APPEND="false"
ARGS=()
DIR="$HOME/export/trips"
DSSPEC=""
SIZE="100"
LINE_CHUNK_SIZE=2500000
LINE_CHUNK_PER_FILE=8
TEMP_CSV_DIR=/tmp/csv-import
USAGE="usage: noms-import [--size 100|1000] [--dir /path/to/csv/files] [--append true|false] dsspec"

lineChunksToSkip=0
filesToSkip=0
chunksToSkip=0

while [[ $# -gt 0 ]]
do
  key="$1"

  case $key in
      -s|--size)
      SIZE="$2"
      shift # past argument
      ;;
      -k|--skip)
      SKIP="$2"
      shift # past argument
      x=$(( $SKIP % $LINE_CHUNK_SIZE ))
      lineChunksToSkip=$(( $SKIP / $LINE_CHUNK_SIZE ))
      filesToSkip=$(( $lineChunksToSkip / $LINE_CHUNK_PER_FILE ))
      chunksToSkip=$(( $lineChunksToSkip % $LINE_CHUNK_PER_FILE ))
      echo "Skipping  $lineChunksToSkip file chunks (total), $filesToSkip files, $chunksToSkip chunks"
      ;;
      -d|--dir)
      DIR="$2"
      shift # past argument
      ;;
      -a|--append)
      APPEND="$2"
      shift # past argument
      ;;
      *)
      ARGS+=($1) # unnamed args
      ;;
  esac
  shift # past argument or value
done

cd $DIR
datafiles=`ls trips_*.csv.gz`
DSSPEC="${ARGS[0]}"

ctr1=0
firstTime=true
for fn in $datafiles
do
  if [[ $cntr1 -ge $filesToSkip ]]
  then
    if [[ $cntr1 -lt 5 ]]
    then
      echo "fn: $fn, bn: $bn"
      rm -rf ${TEMP_CSV_DIR}
      mkdir ${TEMP_CSV_DIR}
      gunzip -c $fn | split - "${TEMP_CSV_DIR}/csv-import-" --lines ${LINE_CHUNK_SIZE} --additional-suffix=.csv

Permission denied (publickey).
#!/usr/bin/env bash

# Args with default values
APPEND="false"
ARGS=()
DIR="$HOME/export/trips"
DSSPEC=""
SIZE="100"
LINE_CHUNK_SIZE=2500000
LINE_CHUNK_PER_FILE=8
TEMP_CSV_DIR=/tmp/csv-import
USAGE="usage: noms-import [--size 100|1000] [--dir /path/to/csv/files] [--append true|false] dsspec"

lineChunksToSkip=0
filesToSkip=0
chunksToSkip=0

while [[ $# -gt 0 ]]
do
  key="$1"

  case $key in
...skipping...
#!/usr/bin/env bash

# Args with default values
APPEND="false"
ARGS=()
DIR="$HOME/export/trips"
DSSPEC=""
SIZE="100"
LINE_CHUNK_SIZE=2500000
LINE_CHUNK_PER_FILE=8
TEMP_CSV_DIR=/tmp/csv-import
USAGE="usage: noms-import [--size 100|1000] [--dir /path/to/csv/files] [--append true|false] dsspec"

lineChunksToSkip=0
filesToSkip=0
chunksToSkip=0

while [[ $# -gt 0 ]]
do
  key="$1"

  case $key in
      -s|--size)
      SIZE="$2"
      shift # past argument
      ;;
      -k|--skip)
      SKIP="$2"
      shift # past argument
      x=$(( $SKIP % $LINE_CHUNK_SIZE ))
      lineChunksToSkip=$(( $SKIP / $LINE_CHUNK_SIZE ))
      filesToSkip=$(( $lineChunksToSkip / $LINE_CHUNK_PER_FILE ))
      chunksToSkip=$(( $lineChunksToSkip % $LINE_CHUNK_PER_FILE ))
      echo "Skipping  $lineChunksToSkip file chunks (total), $filesToSkip files, $chunksToSkip chunks"
      ;;
      -d|--dir)
      DIR="$2"
      shift # past argument
      ;;
      -a|--append)
      APPEND="$2"
      shift # past argument
      ;;
      *)
      ARGS+=($1) # unnamed args
      ;;
  esac
  shift # past argument or value
done

cd $DIR
datafiles=`ls trips_*.csv.gz`
DSSPEC="${ARGS[0]}"

ctr1=0
firstTime=true
for fn in $datafiles
do
  if [[ $cntr1 -ge $filesToSkip ]]
  then
    if [[ $cntr1 -lt 5 ]]
    then
      echo "fn: $fn, bn: $bn"
      rm -rf ${TEMP_CSV_DIR}
      mkdir ${TEMP_CSV_DIR}
      gunzip -c $fn | split - "${TEMP_CSV_DIR}/csv-import-" --lines ${LINE_CHUNK_SIZE} --additional-suffix=.csv

      cntr2=0
      for file in `ls ${TEMP_CSV_DIR}`
      do
        if [[ $cntr2 -ge $chunksToSkip ]]
        then
          csv-import --header=`cat trips.header.csv` --column-types=`cat trips.types.csv` --invert --append=${APPEND} ${TEMP_CSV_DIR}/${file} "${DSSPEC}"
          exitcode=$?
          if [[ $exitcode -ne 0 ]]
          then
            echo "csv-import exited with non-zero status: ${exitcode}"
            exit 1
          fi
          APPEND="true"
        fi
        (( cntr2++ ))
      done
      chunksToSkip=0
    fi
  fi
  (( cntr1++ ))
done