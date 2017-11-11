Clueso
======

Instructions
------------

To build, run:

`./gradlew clean shadowJar -x test`

To run integration tests – requires Docker:

`./gradlew clean test`



Clueso Tool
===========

To build Clueso Tool, invoke:

`./gradlew buildTool`

Tool scripts will be available under ./dist/tool


Table Compactor Tool
--------------------

Format: `./table-compactor.sh <path/to/application.conf> <num parquet files per partition> [<bucket name>]`

**Parameters**

1. path to `application.conf`, that specifies S3 connection settings and compaction settings  
2. number of partitions – set this value to the same as number of spark executors
3. *Optional* bucket name – name of bucket to compact, if none set, will compact all

**Run Example**

Run compaction with 20:

`./table-compactor.sh application.conf 20` 

**Add it to Cronjob**

To run compaction every 24 hours at 1AM:

Run `crontab -e` and select your editor:

Add this to the file

```
0 1 * * * `./table-compactor.sh application.conf 5`
```

**Compact a specific bucket**

To run compaction on a specific bucket:

`./table-compactor.sh application.conf 20 myFatBucket`



Landing Populator Tool
-----------------------

This tool generates fake metadata and can be used prior to a performance test.

Format: `./landing-populator-tool.sh application.conf <bucketName> <num records> <num parquet files>`

**Parameters**

1. path to `application.conf`, that specifies S3 connection settings  
2. bucket name – name of bucket of the generated records
3. number of records – number of records to be generated
4. number of parquet files – number of parquet files to write
 


Run this command to generate 100k metadata entries for bucket `high-traffic-bucket` in landing evenly spread across 100 
parquet files. 

`./landing-populator-tool.sh application.conf high-traffic-bucket 100000 100`



Stats Tool
----------

This tool can report on the number of search metadata files in persistent layer (S3).
This includes average file size of Parquet files, number of records in metadata for a specific bucket and 
number of parquet files in total for that bucket.

`./searchmd-info.sh application.conf <bucketName> [loop=true|false]`

By selecting `loop=true`, it will periodically poll and send information to graphite.

Please set GRAPHITE_HOST and GRAPHITE_PORT 



Performance Testing
===================

The perf_test tool creates files in a bucket with unique random metadata and queries Clueso in loop
until the results for that file arrive. It requires `awscli` to be installed on OS.

It assumes the docker stack is running (via docker-compose or docker swarm).

This evaluates both query speed and latency, which may vary depending if cache is enabled and depending on 
`cache_expiry` in configuration.

Results are published to Graphite and can be visualized using Grafana.
  
`./perf_test.py`

