#!/bin/bash

BUCKET=$1
QUERY=$2

# graphite connection settings
PORT=2003
SERVER=127.0.0.1
TIMEFORMAT=%R

exec 3>&1 4>&2

MEASUREMNT=`{ time node ~/S3/bin/search_bucket.js -a accessKey1 -k verySecretKey1 -b $BUCKET -q $QUERY -h 127.0.0.1 -p 80 1>&3 2>&4; } 2>&1`  # Captures time only.

exec 3>&- 4>&-

# send results to graphite
echo "search_bucket.$BUCKET.time_seconds $MEASUREMNT `date +%s`" | nc $SERVER $PORT --send-only -w1 -t
