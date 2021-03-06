package com.scality.clueso

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.node._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class EventMessageRewriter extends LazyLogging {
  val mapper = new ObjectMapper()
  // needed for the null character (\0 or \u0000) in our versioned key names
  mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)

  def rewriteMsg(bytes: Array[Byte]): String = {
    val payload = new String(bytes)
    try {
      var rootNode = mapper.readTree(payload).asInstanceOf[ObjectNode]

      val valueTxtNode = rootNode.path("value").asInstanceOf[TextNode]

      val valueNodeString = mapper.readTree(valueTxtNode.asText())
      val valueNode = valueNodeString.asInstanceOf[ObjectNode]

      val metadataFieldNames = mutable.ListBuffer[String]()

      val it = valueNode.fieldNames()
      while (it.hasNext) {
        val fieldName = it.next()
        if (fieldName.startsWith("x-amz-meta-")) {
          metadataFieldNames += fieldName
        } else if (fieldName.equals("location")) {
          val locationArray = valueNode.path("location")

          if (!locationArray.isInstanceOf[NullNode] && locationArray.asInstanceOf[ArrayNode].size() > 1) {
            val _locationArray = locationArray.asInstanceOf[ArrayNode]
            (1 until locationArray.size()).foreach(i => _locationArray.remove(i))
            valueNode.replace("location", _locationArray)
          }
        }
      }

      val userMd = valueNode.putObject("userMd")

      for (fieldName <- metadataFieldNames) {
        userMd.set(fieldName, valueNode.path(fieldName).deepCopy().asInstanceOf[JsonNode])
        valueNode.remove(fieldName)
      }

      valueNode.replace("userMd", userMd)
      rootNode.replace("value", valueNode)

      mapper.writeValueAsString(rootNode)
    } catch {
      case e:Throwable =>
        logger.error(s"Error parsing json entry from kafka ${e}")
        ""
    }

    }
}

object EventMessageRewriterWrapper {
  val deser = new EventMessageRewriter
}

object MetadataIngestionPipeline extends LazyLogging {

  val msgRewriteFun = (bytes: Array[Byte]) =>
    EventMessageRewriterWrapper.deser.rewriteMsg(bytes)

  val findNextMaxOpIndexFun = (compactionRecordInterval:Long, value : String) => {
    // op index format = "<12 0-padded record number>_<index number>"
    val recordNo = value.substring(0, 12).toLong

    if (recordNo % compactionRecordInterval == 0) {
      recordNo
    } else {
      recordNo + compactionRecordInterval - recordNo % compactionRecordInterval
    }
  }

  import org.apache.spark.sql.functions.udf
  val msg_rewrite = udf(msgRewriteFun)
  val find_next_max_op_index = udf(findNextMaxOpIndexFun)

  /**
    * Applies projections and conditions to incoming data frame
    * Ignores malformed json and garbage payloads
    * Ignores events from specified bucket name
    *
    * @param bucketNameToFilterOut
    * @param eventStream
    * @return
    */
  def filterAndParseEvents(bucketNameToFilterOut : String, eventStream: DataFrame)(implicit spark : SparkSession, config: CluesoConfig) = {

    var df = eventStream.select(
      msg_rewrite(col("value")).as("content")
    )

    // defensive filtering to not process kafka garbage
    df = df.filter(col("content").isNotNull)
      // msg_rewrite will return an empty string if parsing json throws
          .filter(length(col("content")).gt(3))
          .select(
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
      .withColumn("opIndex", col("event.opIndex"))
      .withColumn("maxOpIndex", find_next_max_op_index(lit(config.compactionRecordInterval), col("event.opIndex")))
      .withColumn("message", col("event.value"))

    df = df.filter(
      !col("bucket").eqNullSafe(bucketNameToFilterOut) &&
        !col("bucket").eqNullSafe("users..bucket") &&
        !col("bucket").eqNullSafe("__metastore") &&
        !col("bucket").eqNullSafe("PENSIEVE") &&
        (col("bucket").isNotNull && !col("bucket").startsWith("mpuShadowBucket"))
    )

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


    implicit val spark = SparkUtils.buildSparkSession(config)
      .appName("Metadata Ingestion Pipeline")
      .getOrCreate()

    val eventStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", config.kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()


    val filteredEventsDf = filterAndParseEvents(config.bucketName, eventStream)

    val writeStream = filteredEventsDf.writeStream
      .trigger(ProcessingTime(config.triggerTime.toMillis))

    val query = writeStream
      .option("checkpointLocation", config.checkpointUrl)
      .format("parquet")
      .partitionBy("bucket", "maxOpIndex")
      .outputMode(OutputMode.Append())
      .option("path", PathUtils.landingURI)
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
