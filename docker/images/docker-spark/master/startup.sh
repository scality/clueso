#!/bin/bash
set -eo pipefail

# s3 secret credentials for Zenko
if [ -r /run/secrets/s3-credentials ] ; then
    echo "Reading S3 credentials from secrets"
    . /run/secrets/s3-credentials

    echo "aws_access_key_id = \"$AWS_ACCESS_KEY_ID\"" >> /clueso/conf/application.conf
    echo "aws_secret_access_key = \"$AWS_SECRET_ACCESS_KEY\"" >> /clueso/conf/application.conf

fi

echo "Starting cron"
cron start

echo "Running python"
python runTasks.py || exit 1

if curl --fail -X POST --output /dev/null --silent --head http://127.0.0.1:8080; then
     printf 'Waiting for Spark Master...'
     until $(curl --output /dev/null --silent --head --fail http://127.0.0.1:8080); do
          printf '.'
          sleep 1
     done
fi

export SPARK_MASTER_HOST=`hostname`

# #Supervisor will monitor the ingestion to ensure it stays up
supervisord -c /etc/supervisor/supervisord.conf -n
