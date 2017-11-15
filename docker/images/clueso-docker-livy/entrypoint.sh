#!/bin/bash

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

$LIVY_APP_PATH/bin/livy-server $@
