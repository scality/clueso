#!/bin/bash
set -eo pipefail

if curl --fail -X POST --output /dev/null --silent --head http://lb; then
     printf 'Waiting for S3 LB...'
     until $(curl --output /dev/null --silent --head --fail http://lb); do
          printf '.'
          sleep 1
     done
fi


# s3 secret credentials for Zenko
./getPensieveCreds-linux clueso | tail -n 2 >> ~/.bashrc

source ~/.bashrc

echo "aws_access_key_id = \"$AWS_ACCESS_KEY_ID\"" >> /clueso/conf/application.conf
echo "aws_secret_access_key = \"$AWS_SECRET_ACCESS_KEY\"" >> /clueso/conf/application.conf

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
