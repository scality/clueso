#!/bin/bash

  . "/spark/sbin/spark-config.sh"

  . "/spark/bin/load-spark-env.sh"

  mkdir -p $SPARK_WORKER_LOG

# wait for Spark Master to be live
echo "Waiting for Spark Master to launch on 7077..."

while ! nc -z spark-master 7077 &>/dev/null; do   
  echo "Cannot contact spark master on 7077"
  exit 1
done

echo "Spark Master Ready"
sleep 3
echo "Starting Spark Worker"

# start Spark Worker
  /spark/sbin/../bin/spark-class org.apache.spark.deploy.worker.Worker \
    --webui-port $SPARK_WORKER_WEBUI_PORT $SPARK_MASTER >> $SPARK_WORKER_LOG/spark-worker.out
