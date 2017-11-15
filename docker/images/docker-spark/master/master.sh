#!/bin/bash

# s3 secret credentials for Zenko
if [ -r /run/secrets/s3-credentials ] ; then
    echo "Reading S3 credentials from secrets"
    . /run/secrets/s3-credentials
fi

echo "Starting cron"
cron start

python runTasks.py || exit 1

if curl --fail -X POST --output /dev/null --silent --head http://127.0.0.1:8080; then
     printf 'Waiting for Spark Master...'
     until $(curl --output /dev/null --silent --head --fail http://127.0.0.1:8080); do
          printf '.'
          sleep 5
     done
fi

echo "Executing Clueso Pipeline in background..."

java -cp /spark/conf:/spark/jars/* \
     -Xmx512m org.apache.spark.deploy.SparkSubmit \
     --conf spark.executor.memory=512m \
     --conf spark.driver.memory=512m \
     --conf spark.master=spark://spark-master:7077 \
     --conf spark.driver.cores=1 \
     --conf spark.cores.max=2 \
     --conf spark.executor.cores=1 \
     --queue default \
     --class com.scality.clueso.MetadataIngestionPipeline \
     --name "Clueso Metadata Ingestion Pipeline" \
     file:///clueso/clueso.jar /clueso/application.conf &

export SPARK_MASTER_IP=`hostname`

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"

mkdir -p $SPARK_MASTER_LOG

/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host $SPARK_MASTER_IP --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG/spark-master.out


