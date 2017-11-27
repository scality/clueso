#!/bin/bash
set -e -o pipefail

# s3 secret credentials for Zenko
if [ -r /run/secrets/s3-credentials ] ; then
    echo "Reading S3 credentials from secrets"
    . /run/secrets/s3-credentials

    echo "spark.hadoop.fs.s3a.access.key  $AWS_ACCESS_KEY_ID" >> /spark/conf/spark-defaults.conf
    echo "spark.hadoop.fs.s3a.secret.key  $AWS_SECRET_ACCESS_KEY" >> /spark/conf/spark-defaults.conf
fi

if [ -z ${HOST+x} ]; then 
# if host isn't known then check outbound IP when contacting google
  export LIBPROCESS_IP=$(ip route get 8.8.8.8 | head -1 | cut -d' ' -f8)
else
  export LIBPROCESS_IP=$HOST
fi

# wait for Spark master before starting livy
echo "Waiting for Spark Master to launch on 7077..."

while ! nc -z spark-master 7077 &> /dev/null; do   
  sleep 1 # wait for 1 second before check again
done

echo "Spark Master Ready"
sleep 3
echo "Starting Livy Server"

# start Livy
$LIVY_APP_PATH/bin/livy-server $@
