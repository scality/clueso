#!/bin/bash
set -eo pipefail

  . "/spark/sbin/spark-config.sh"

  . "/spark/bin/load-spark-env.sh"

  mkdir -p $SPARK_WORKER_LOG

# exit if spark master is not ready
if [ ! nc -z spark-master 7077 &>/dev/null ]; then   
  echo "Cannot contact spark master on 7077"
  exit 1
fi

echo "Starting Spark Worker"
# start Spark Worker
  /spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG/spark-worker.out
