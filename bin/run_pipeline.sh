#!/bin/bash



livyget() { curl 127.0.0.1:8998/$1 | python -m json.tool; }
livyput() { curl -X POST --data "$2" -H "Content-Type: application/json" 127.0.0.1:8998/$1  | python -m json.tool; }


livyput batches "{ \"file\": \"file:///apps/spark-modules/clueso-1.0-SNAPSHOT-all.jar\", \"className\": \"com.scality.clueso.MetadataIngestionPipeline\", \"name\": \"Clueso Metadata Ingestion Pipeline\", \"executorCores\":1, \"executorMemory\":\"512m\", \"driverCores\":1, \"driverMemory\":\"512m\", \"queue\":\"default\", \"args\": [\"/apps/spark-modules/application.conf\"], \"conf\": {\"spark.driver.port\":\"38600\", \"spark.cores.max\": \"2\",\"spark.sql.parquet.cacheMetadata\":\"false\"}}"
