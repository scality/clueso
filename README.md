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

**Parameters**

1. path to `application.conf`, that specifies S3 connection settings and compaction settings  
2. number of partitions – set this value to the same as number of spark executors

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



Stats Tool
----------

This tool can report on the number of search metadata files in persistent layer (S3).
This includes average file size of Parquet files, number of records in metadata for a specific bucket and 
number of parquet files in total for that bucket.

`./searchmd-info.sh application.conf <bucketName> [loop=true|false]`

By selecting `loop=true`, it will periodically poll and send information to graphite.

Please set GRAPHITE_HOST and GRAPHITE_PORT 











