package com.scality.clueso

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}

object MetadataIngestionPipeline {
  def main(args: Array[String]): Unit = {
    require(args.length > 0, "specify configuration file")

    val parsedConfig = ConfigFactory.parseFile(new File(args(0)))
    val _config = ConfigFactory.load(parsedConfig)

    val config = new CluesoConfig(_config)

    val spark = SparkSession
      .builder
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
      .config("spark.hadoop.fs.s3a.endpoint", config.s3Endpoint)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", config.s3AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", config.s3SecretKey)
      .master("local[*]")
      .appName("Metadata Ingestion Pipeline")
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

    val bucketName = config.bucketName

    val writeStream = eventStream.select(trim(col("value").cast("string")).as("content"))
      .filter(col("content").isNotNull)
      .filter(length(col("content")).gt(3))
      .select(
        from_json(col("content").cast("string"), CluesoConstants.sstSchema)
          .alias("message")
      )
      .filter(col("message").isNotNull)
      .withColumn("bucket",
        when(
          col("message").isNotNull.and(
            col("message.bucket").isNotNull
          ), col("message.bucket")).otherwise("NOBUCKET")
      )
      .filter(!col("bucket").eqNullSafe(bucketName))

      .writeStream
      .trigger(ProcessingTime(config.triggerTime.toMillis))

    val query = writeStream
      .option("checkpointLocation", config.checkpointPath)
      .format("parquet")
      .partitionBy("bucket")
      .outputMode(OutputMode.Append())
      .option("path", config.landingPath)
      .start()

    query.awaitTermination()
  }

  def printConfig(config : Config) = {
    import com.typesafe.config.ConfigRenderOptions
    val renderOpts = ConfigRenderOptions.defaults().setOriginComments(false).setComments(false).setJson(false)
    println(" ===== CONFIG  =====")
    println(config.root().render(renderOpts))
    println(" ===== END OF CONFIG  =====")
  }
}
