package com.scality.clueso

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.ProcessingTime

object MetadataIngestionPipeline {
  def main(args: Array[String]): Unit = {
    val config = new CluesoConfig(ConfigFactory.load)

    val spark = SparkSession
      .builder
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
      .config("spark.hadoop.fs.s3a.endpoint", config.s3Endpoint)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", config.s3AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", config.s3SecretKey)
      .master("local[*]")
      .appName("S3ATest")
      .getOrCreate()


    import org.apache.spark.sql.functions._

    val eventStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", config.kafkaTopic)
      // TODO custom offsets depending on recovery point
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()


    val writeStream = eventStream.select(
        from_json(col("value").cast("string"), CluesoConstants.sstSchema)
          .alias("message")
      )
      .filter(row => row.getStruct(row.fieldIndex("message")) != null)
      .withColumn("bucket", col("message.bucket"))
      .writeStream
      .trigger(ProcessingTime(config.triggerTime.toMillis))

    val query = writeStream
      .option("checkpointLocation", config.checkpointPath)
      .format("parquet")
      .partitionBy("bucket")
      .option("path", config.outputPath)
      .start()

    query.awaitTermination()
  }
}
