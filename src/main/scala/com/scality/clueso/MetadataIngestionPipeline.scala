package com.scality.clueso

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.scality.clueso.merge.MergeService
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

object MetadataIngestionPipeline extends LazyLogging {
  val mapper = new ObjectMapper()

  // assumes to have payload on index 0
  def buildJsonUserMetadataObject: Row => Row = row => {
    val payload = row.getString(0)

    var rootNode = mapper.readTree(payload).asInstanceOf[ObjectNode]
    rootNode = rootNode.path("value").asInstanceOf[ObjectNode]

    val metadataFieldNames = mutable.ListBuffer[String]()

    val it = rootNode.fieldNames()
    while (it.hasNext) {
      val fieldName = it.next()
      if (fieldName.startsWith("x-amz-meta-")) {
        metadataFieldNames += fieldName
      } else if (fieldName.equals("location")) {
        val locationArray = rootNode.path("location").asInstanceOf[ArrayNode]
        // TODO test
        if (locationArray != null && locationArray.size() > 1) {
          (1 until locationArray.size()).foreach(i => locationArray.remove(i))
        }
      }
    }

    val userMd = if (rootNode.has("userMd")) {
      rootNode.path("userMd").asInstanceOf[ObjectNode]
    } else {
      rootNode.putObject("userMd")
    }

    for (fieldName <- metadataFieldNames) {
      userMd.set(fieldName, rootNode.path(fieldName).deepCopy().asInstanceOf[JsonNode])
      rootNode.remove(fieldName)
    }

    Row(mapper.writeValueAsString(rootNode))
  }


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
    var df = eventStream.select(
      col("timestamp").cast(TimestampType).as("kafkaTimestamp"),
      trim(col("value").cast(StringType)).as("content")
    )

    // defensive filtering to not process kafka garbage
    df = df.filter(col("content").isNotNull)
          .filter(length(col("content")).gt(3))
          .select(col("content"))
          .map(buildJsonUserMetadataObject)
          .select(
            col("kafkaTimestamp"),
            from_json(col("content"), CluesoConstants.eventSchema).alias("event")
          )

    df = df.filter(col("event").isNotNull.and(col("event.type").isNotNull))
      .withColumn("key", when(
        col("event").isNotNull.and(
          col("event.key").isNotNull
        ), col("event.key")).otherwise("")
      )
      .withColumn("bucket",
        when(
          col("event").isNotNull.and(
            col("event.bucket").isNotNull
          ), col("event.bucket")).otherwise("NOBUCKET")
      )
      .withColumn("type", col("event.type"))
      .withColumn("message", col("event.value"))

    df = df.filter(!col("bucket").eqNullSafe(bucketNameToFilterOut))
    df.drop("event")
  }

  def main(args: Array[String]): Unit = {
    require(args.length > 0, "specify configuration file")

    implicit val config = SparkUtils.loadCluesoConfig(args.head)

    // create dir no matter what
    val fs = SparkUtils.buildHadoopFs(config)

    logger.info(s"Creating directory ${PathUtils.landingURI}")
    logger.info(s"Creating directory ${PathUtils.stagingURI}")

    fs.mkdirs(new Path(PathUtils.landingURI))
    fs.mkdirs(new Path(PathUtils.stagingURI))


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
      .option("checkpointLocation", config.checkpointUrl)
      .format("parquet")
      .partitionBy("bucket")
      .outputMode(OutputMode.Append())
      .option("path", PathUtils.landingURI)
      .start()



//    val maxRecordNumber = eventStream.select(col("opIndex"))
//        .map(row => {
//          val value = row.getString(row.fieldIndex("opIndex"))
//          value.substring(0, 12).toLong
//        }).rdd.max()

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
