#/bin/bash


livyget() { curl 127.0.0.1:8998/$1 | python -m json.tool; }
livyput() { curl -X POST --data "$2" -H "Content-Type: application/json" 127.0.0.1:8998/$1  | python -m json.tool; }

export CURRENT_SESSION=$((${CURRENT_SESSION:--1} + 1))
echo "Starting session $CURRENT_SESSION";

livyput sessions "{\"kind\":\"spark\",\"numExecutors\":2,\"executorMemory\":\"512m\",\"jars\":[\"/apps/spark-modules/clueso-1.0-SNAPSHOT-all.jar\"], \"conf\": { \"spark.hadoop.fs.s3a.impl\": \"org.apache.hadoop.fs.s3a.S3AFileSystem\", \"spark.hadoop.fs.s3a.connection.ssl.enabled\": \"false\", \"spark.hadoop.fs.s3a.endpoint\": \"10.0.1.15\", \"spark.hadoop.fs.s3a.access.key\": \"accessKey1\", \"spark.hadoop.fs.s3a.secret.key\": \"verySecretKey1\", \"spark.hadoop.fs.s3a.path.style.access\": \"true\", \"spark.driver.extraJavaOptions\": \"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005\", \"spark.sql.parquet.cacheMetadata\":\"false\",\"spark.cores.max\":\"2\",\"spark.driver.port\":\"38600\"}}";


sleep 17;

livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"import com.scality.clueso._\"}";
livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"import com.scality.clueso.query._\"}";


livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"val config = com.scality.clueso.SparkUtils.loadCluesoConfig(\\\"/apps/spark-modules/application.conf\\\"); config \"}"
livyget sessions/$CURRENT_SESSION/statements
livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"val query = new MetadataQuery(spark, config, \\\"wednesday\\\", \\\"\\\", 0, 1000)\"}"

livyget sessions/$CURRENT_SESSION/statements



livyput sessions/$CURRENT_SESSION/statements "{\"code\":\"SparkUtils.getQueryResults(spark,query)\"}"
livyget sessions/$CURRENT_SESSION/statements