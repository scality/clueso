#!/bin/bash
set -eo pipefail

  . "/spark/sbin/spark-config.sh"

  . "/spark/bin/load-spark-env.sh"

  mkdir -p $SPARK_WORKER_LOG

# Wait for spark master
# This allows clean startup
RETRY=15
echo "Waiting for spark master"
while ! nc -z spark-master 7077 &>/dev/null ; do
  RETRY=$((RETRY-1))
  if [ $RETRY == 0 ]; then
    echo "Cannot contact spark master on 7077"
    exit 1
  fi
  sleep 1
done

echo "Starting Spark Worker"
# start Spark Worker
  /spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER_HOST >> $SPARK_WORKER_LOG/spark-worker.out
