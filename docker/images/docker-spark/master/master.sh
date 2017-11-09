#!/bin/bash

python runTasks.py || exit 1

if curl --fail -X POST --output /dev/null --silent --head http://127.0.0.1:8080; then
     printf 'Waiting for spark...'
     until $(curl --output /dev/null --silent --head --fail http://127.0.0.1:8080); do
          printf '.'
          sleep 5
     done
fi

java -cp /spark/conf:/spark/jars/* \
     -Xmx512m org.apache.spark.deploy.SparkSubmit \
     --conf spark.executor.memory=512m \
     --conf spark.driver.memory=512m \
     --conf spark.master=spark://spark-master:7077 \
     --conf spark.driver.cores=1 --conf spark.executor.cores=1 --class com.scality.clueso.MetadataIngestionPipeline \
     --name "Clueso Metadata Ingestion Pipeline" \
     --queue default file:///clueso/clueso.jar /clueso/application.conf \
     --conf spark.cores.max=2

export SPARK_MASTER_IP=`hostname`

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"

mkdir -p $SPARK_MASTER_LOG

/spark/bin/spark-class org.apache.spark.deploy.master.Master \
    --host $SPARK_MASTER_IP --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG/spark-master.out


