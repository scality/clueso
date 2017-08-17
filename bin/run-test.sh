#!/bin/bash

export CURRENT_SESSION=${CURRENT_SESSION:-0}

export LIVY_URL=${LIVY_URL:-127.0.0.1:8998}
export S3_SERVER=${S3_SERVER:-127.0.0.1}

livyget() { curl $LIVY_URL/$1 | python -m json.tool; }
livyput() { curl -X POST --data "$2" -H "Content-Type: application/json" $LIVY_URL/$1  | python -m json.tool; }


livyput sessions "{\"kind\":\"spark\",\"numExecutors\":2,\"executorMemory\":\"512m\",\"jars\":[\"/apps/spark-modules/clueso-1.0-SNAPSHOT-all.jar\"], \"conf\": { \"spark.hadoop.fs.s3a.impl\": \"org.apache.hadoop.fs.s3a.S3AFileSystem\", \"spark.hadoop.fs.s3a.connection.ssl.enabled\": \"false\", \"spark.hadoop.fs.s3a.endpoint\": \"$S3_SERVER\", \"spark.hadoop.fs.s3a.access.key\": \"accessKey1\", \"spark.hadoop.fs.s3a.secret.key\": \"verySecretKey1\", \"spark.hadoop.fs.s3a.path.style.access\": \"true\", \"spark.driver.extraJavaOptions\": \"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005\", \"spark.sql.parquet.cacheMetadata\":\"false\"}}";

sleep 12;

livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"import com.scality.clueso._\"}";
livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"import com.scality.clueso.query._\"}";


livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"val config = com.scality.clueso.SparkUtils.loadCluesoConfig(\\\"/apps/spark-modules/application.conf\\\"); config \"}"
livyget sessions/$CURRENT_SESSION/statements
livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"val query = new MetadataQuery(spark, config, \\\"wednesday\\\", \\\"\\\", 0, 1000)\"}"

livyget sessions/$CURRENT_SESSION/statements



livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"SparkUtils.getQueryResults(spark,query)\"}"
livyget sessions/$CURRENT_SESSION/statements