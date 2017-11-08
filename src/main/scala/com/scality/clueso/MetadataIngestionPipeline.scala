package com.scality.clueso

import com.fasterxml.jackson.databind.node.{ArrayNode, NullNode, ObjectNode, TextNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, OutputMode, ProcessingTime}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

class EventMessageRewriter {
  val mapper = new ObjectMapper()

  def rewriteMsg(bytes: Array[Byte]): String = {
    val payload = new String(bytes)

    var rootNode = mapper.readTree(payload).asInstanceOf[ObjectNode]

    val valueTxtNode = rootNode.path("value").asInstanceOf[TextNode]

    val valueNode = mapper.readTree(valueTxtNode.asText()).asInstanceOf[ObjectNode]

    val metadataFieldNames = mutable.ListBuffer[String]()

    val it = valueNode.fieldNames()
    while (it.hasNext) {
      val fieldName = it.next()
      if (fieldName.startsWith("x-amz-meta-")) {
        metadataFieldNames += fieldName
      } else if (fieldName.equals("location")) {
        val locationArray = valueNode.path("location")

        if (!locationArray.isInstanceOf[NullNode] && locationArray.asInstanceOf[ArrayNode].size() > 1) {
          val _locationArray =  locationArray.asInstanceOf[ArrayNode]
          (1 until locationArray.size()).foreach(i => _locationArray.remove(i))
          valueNode.replace("location", _locationArray)
        }
      }
    }

    val userMd = if (rootNode.has("userMd")) {
      valueNode.path("userMd").asInstanceOf[ObjectNode]
    } else {
      valueNode.putObject("userMd")
    }

    for (fieldName <- metadataFieldNames) {
      userMd.set(fieldName, valueNode.path(fieldName).deepCopy().asInstanceOf[JsonNode])
      valueNode.remove(fieldName)
    }

    valueNode.replace("userMd", userMd)
    rootNode.replace("value", valueNode)

    mapper.writeValueAsString(rootNode)
  }
}

object EventMessageRewriterWrapper {
  val deser = new EventMessageRewriter
}

object MetadataIngestionPipeline extends LazyLogging {

  val msgRewriteFun = (bytes: Array[Byte]) =>
    EventMessageRewriterWrapper.deser.rewriteMsg(bytes)

//  val compactionRecordInterval = config.compactionRecordInterval

  val maxOpIndexFun = (compactionRecordInterval:Long, value : String) => {
    val recordNo = value.substring(0, 12).toLong

    if (recordNo % compactionRecordInterval == 0) {
      recordNo
    } else {
      recordNo + compactionRecordInterval - recordNo % compactionRecordInterval
    }
  }

  import org.apache.spark.sql.functions.udf
  val msg_rewrite = udf(msgRewriteFun)

  val max_op_index = udf(maxOpIndexFun)

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
      col("timestamp").cast(TimestampType).as("kafkaTimestamp"),
      msg_rewrite(col("value")).as("content")
    )

    // defensive filtering to not process kafka garbage
    df = df.filter(col("content").isNotNull)
          .filter(length(col("content")).gt(3))
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
      .withColumn("opIndex", col("event.opIndex"))
      .withColumn("maxOpIndex", max_op_index(lit(config.compactionRecordInterval), col("event.opIndex")))
      .withColumn("message", col("event.value"))

    df = df.filter(
      !col("bucket").eqNullSafe(bucketNameToFilterOut) &&
        !col("bucket").eqNullSafe("users..bucket") &&
        !col("bucket").eqNullSafe("__metastore") &&
        (col("bucket").isNotNull && !col("bucket").startsWith("mpuShadowBucket")))

//      df = df.filter(!col("bucket").eqNullSafe(bucketNameToFilterOut))

      df.drop("event")
  }

  def stateFun(recordNo: Long, it: Iterator[Long], state: GroupState[List[Long]]): Option[Long] = {
    if (!state.hasTimedOut) {
      if (state.getOption.isEmpty) {
        state.update(List[Long](recordNo))
      } else {
        state.update(state.get ++ List(recordNo))
      }
    }

    Some(recordNo)
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

    import spark.implicits._

    //    val mergerService = new MergeService(spark, config)

    val eventStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", config.kafkaTopic)
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()


    val filteredEventsDf = filterAndParseEvents(config.bucketName, eventStream)

    val compactionRecordInterval = config.compactionRecordInterval
    val compactionTriggerDf = filteredEventsDf
      .select(col("opIndex"))
      .as[String]
      .flatMap(value => {
        val recordNo = value.substring(0, 12).toLong

        if (recordNo > 0 && recordNo % compactionRecordInterval == 0) {
          Some(recordNo)
        } else {
          None
        }
      })


    compactionTriggerDf
      .groupByKey(x => x)
      .mapGroupsWithState[List[Long], Option[Long]](stateFun _)
      .foreach(recordNo => {
        logger.info(s" TRIGGER: ${recordNo}")
      })

    val writeStream = filteredEventsDf.writeStream
      .trigger(ProcessingTime(config.triggerTime.toMillis))


    val query = writeStream
      .option("checkpointLocation", config.checkpointUrl)
      .format("parquet")
      .partitionBy("bucket")
      .outputMode(OutputMode.Append())
      .option("path", PathUtils.landingURI)
      .start()

    // TODO reimplement this with state

//    compactionTriggerDf.collect.foreach(recordNo => {
//      // trigger compaction async
//      logger.info(s"Compaction trigger for recordNo = ${recordNo}")
//      // TODO
//    })

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
