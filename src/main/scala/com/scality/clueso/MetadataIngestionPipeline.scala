package com.scality.clueso

import com.scality.clueso.merge.MergeService
import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.types.{StringType, TimestampType}

object MetadataIngestionPipeline {

  /**
    * Applies projections and conditions to incoming data frame
    * Ignores malformed json and garbage payloads
    * Ignores events from specified bucket name
    *
    * @param bucketNameToFilterOut
    * @param eventStream
    * @return
    */
  def filterAndParseEvents(bucketNameToFilterOut : String, eventStream: DataFrame) = {
    eventStream.select(
      col("timestamp").cast(TimestampType).as("kafkaTimestamp"),
      trim(col("value").cast(StringType)).as("content")
    )
      // defensive filtering to not process kafka garbage
      .filter(col("content").isNotNull)
      .filter(length(col("content")).gt(3))
      .select(
        col("kafkaTimestamp"),
        from_json(col("content"), CluesoConstants.eventSchema)
          .alias("message")
      )
      .filter(col("message").isNotNull)
      .withColumn("bucket",
        when(
          col("message").isNotNull.and(
            col("message.bucket").isNotNull
          ), col("message.bucket")).otherwise("NOBUCKET")
      )
      .filter(!col("bucket").eqNullSafe(bucketNameToFilterOut))
  }

  def main(args: Array[String]): Unit = {
    require(args.length > 0, "specify configuration file")

    val config = SparkUtils.loadCluesoConfig(args.head)

    // create dir no matter what
    val fs = SparkUtils.buildHadoopFs(config)
    fs.mkdirs(new Path(config.landingPath))
    fs.mkdirs(new Path(config.stagingPath))

    val spark = SparkUtils.buildSparkSession(config)
      .appName("Metadata Ingestion Pipeline")
      .getOrCreate()

    val mergerService = new MergeService(spark, config)

    val eventStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", config.kafkaTopic)
      // TODO custom offsets depending on recovery point
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()

    val writeStream = filterAndParseEvents(config.bucketName, eventStream).writeStream
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
