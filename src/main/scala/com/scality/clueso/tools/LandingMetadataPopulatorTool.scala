package com.scality.clueso.tools

import java.io.File
import java.sql.Timestamp
import java.util.{Date, UUID}

import com.scality.clueso.MetadataIngestionPipeline.find_next_max_op_index
import com.scality.clueso._
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{col, lit}

import scala.util.Random

object LandingMetadataPopulatorTool extends LazyLogging {

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Usage: ./landing-populator-tool.sh application.conf <bucketName> <num records> <num parquet files>")

    val parsedConfig = ConfigFactory.parseFile(new File(args(0)))
    val _config = ConfigFactory.load(parsedConfig)

    implicit val config = new CluesoConfig(_config)
    config.setSparkUiPort(4051)
    val spark = SparkUtils.buildSparkSession(config)
      .master("local[*]")
      .appName("")
      .getOrCreate()


    val bucketName = args(1)
    val totalNumRecords = args(2).toLong
    val totalNumFiles = args(3).toLong

    val fs = SparkUtils.buildHadoopFs(config)

    val landingBucketPath = s"${PathUtils.landingURI}/bucket=$bucketName"

    if (fs.exists(new Path(s"$landingBucketPath/"))) {
      logger.info(s"Deleting landing path: $landingBucketPath")
      fs.delete(new Path(landingBucketPath), true)
    }

    val numRecordsPerPartition = List.fill(Math.max(totalNumFiles - 1, 0).toInt)(totalNumRecords / totalNumFiles) ++
      Array(totalNumRecords / totalNumFiles + totalNumRecords % totalNumFiles)

    val partitionsRdd = spark.sparkContext.parallelize(numRecordsPerPartition, totalNumFiles.toInt)

    val compactionRecordInterval = config.compactionRecordInterval
    val generatedData = partitionsRdd.mapPartitions(it => {
      val prefix = UUID.randomUUID().toString.substring(0, 4)

      val numRecords = it.next.toInt
      (1 to numRecords).map { recordNo =>
        val key = s"${prefix}_${recordNo}"
        val food = if (Random.nextBoolean()) { "pizza" } else { "pasta" }
        val userMd = Map("x-amz-meta-food" -> food, "x-amz-meta-random" -> Random.nextInt(10).toString)

        val eventType = "put"

        val message = new GenericRowWithSchema(Array(
          userMd, bucketName, key,
          new GenericRowWithSchema(Array("private", Array[String](), Array[String](), Array[String](), Array[String]()), CluesoConstants.eventAclSchema), // acls
          Array(), // locations
          Map[String, String](), // tags
          new GenericRowWithSchema(Array("", Array[String](), "", "", ""), CluesoConstants.replicationInfoSchema), // replicationInfo
          1, // md-model-version
          "", // owner-display-name", StringType)
          "", // owner-id", StringType)
          Random.nextInt(200), // content-length", IntegerType)
          "", // content-type
          new Timestamp(new Date().getTime), // last-modified"
          "4b02d12ad7f063d67aec9dc2116a57a2", // content-md5
          "", // x-amz-server-version-id
          "scalitycloud-east-1", //dataStoreName
          "", // x-amz-storage-class
          "", // x-amz-server-side-encryption
          "", // x-amz-server-side-encryption-aws-kms-key-id
          "", // x-amz-server-side-encryption-customer-algorithm
          "", // x-amz-website-redirect-location
          false, // isDeleteMarker
          "" // x-amz-version-id
        ), CluesoConstants.eventValueSchema)

        val opIndex = "%012d_%d".format(recordNo,Random.nextInt(200))
        val nextOpIndex = MetadataIngestionPipeline.findNextMaxOpIndexFun(compactionRecordInterval, opIndex).toString

        val values : Array[Any] = Array(bucketName, key, opIndex, nextOpIndex,
          eventType, message)

        new GenericRowWithSchema(values, CluesoConstants.storedEventSchema).asInstanceOf[Row]

      }.iterator
    })

    val generatedDataDf = spark.createDataFrame(generatedData, CluesoConstants.storedEventSchema)
      .withColumn("maxOpIndex", find_next_max_op_index(lit(config.compactionRecordInterval), col("opIndex")))
      .cache()

    generatedDataDf
      .write
      .partitionBy("maxOpIndex")
      .parquet(landingBucketPath)

    logger.info("Written %d records across %d partitions", generatedDataDf.count, generatedDataDf.rdd.getNumPartitions)
  }
}
