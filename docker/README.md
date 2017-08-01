Instructions
============

Place spark app jars under jars dir:

`cp /path/to/spark-examples_2.11-2.1.1.jar ./jars`



Run `docker-compose -d up`


Livy is running on http://localhost:8998/ and is executing Spark jobs in distributed mode.


To test, run:

`curl -X POST -H "Content-Type: application/json" --data '{ "file": "file:///apps/spark-modules/spark-examples_2.11-2.1.1.jar", "className": "org.apache.spark.examples.SparkPi", "name": "Scala Livy Pi Example", "executorCores":1, "executorMemory":"512m", "driverCores":1, "driverMemory":"512m", "queue":"default", "args":["100"]}' http://localhost:8998/batches`


