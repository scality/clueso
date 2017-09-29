Clueso
======


Build Clueso Tools
------------------

To build Clueso Tool, invoke:

`./gradlew buildTool`

Tool will be available under ./dist/tool.



Table Merger Tool
-----------------

To merge a bucket into a specified number of partitions:
 
`./table-merger.sh <path/to/application.conf> <bucket name> <num partitions>`  


To check eligibility to merge:

`./table-merger.sh <path/to/application.conf> <bucket name> -t`


Cache Tool
----------

This tool can check for locks in the cache layer (Alluxio). 
 
`./cache-tool.sh <application.conf> <operation> <op args>`

Operations:
 - `lockdel <bucketName>` – Deletes the lock files for /bucketName/ cache computation


Storage Tool
------------

This tool can report on the number of search metadata files in persistent layer (S3).
This includes average file size of Parquet files, number of records in metadata for a specific bucket and 
number of parquet files in total for that bucket.

`./searchmd-info.sh application.conf <bucketName> [loop=true|false]`

By selecting `loop=true`, it will periodically poll and send information to graphite.


