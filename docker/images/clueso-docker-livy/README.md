# Livy Docker Image

Builds Livy from source and bundles with Spark installation.

- Spark 2.1.1 with Hadoop 2.8 support (required for path-style S3 interaction)
- Spark compiled with Scala 2.11
- OpenJDK 8
- Livy 0.4.0-incubating

Build
-----

`docker build -t scality/clueso-livy:0.4.0 .`

