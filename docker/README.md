Instructions
============

Place spark app jars under jars dir:

`cp build/libs/clueso-1.0-SNAPSHOT-all.jar ./docker/jars`

Run `docker-compose -f ./docker/docker-compose.yml -d up`


Livy is running on http://localhost:8998/ and is executing Spark jobs in distributed mode, using Spark containers (master and 2 workers).


To spin up the Metadata Ingestion Pipeline: 

curl -X POST -H "Content-Type: application/json" --data '{ "file": "file:///apps/spark-modules/clueso-1.0-SNAPSHOT-all.jar", "className": "com.scality.clueso.MetadataIngestionPipeline", "name": "Scala Livy Pi Example", "executorCores":1, "executorMemory":"512m", "driverCores":1, "driverMemory":"512m", "queue":"default"}' http://localhost:8998/batches


# Metadata Ingestion Pipeline

The ingestion pipeline consumes events from Kafka and writes them as parquet in configured landing area.

The ingestion pipeline runs a MergeService that is triggered periodically. The MergeService performs a per-partition merge between landing and staging tables, whenever certain criteria are met:
- at least 2 Parquet files in landing table partition
- number of parquet files in staging + landing tables, for a given partition, are greater than optimalNumberOfPartitions + 2

`optimalNumberOfPartitions =  ceil(total records in staging partition + landing partitition / mergeFactor`)

