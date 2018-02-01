package com.scality.clueso.tools

import java.io.File
import java.net.Socket
import java.util.Date

import com.scality.clueso.query.MetadataQueryExecutor
import com.scality.clueso.{CluesoConfig, SparkUtils}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object MetadataStorageInfoTool extends LazyLogging {
  lazy val graphiteHost = sys.props.getOrElse("graphiteHost", "graphite")
  lazy val graphitePort = sys.props.getOrElse("graphitePort", "2003").toInt

  def processMetrics(metricName: String, measurement: Long, sendToGraphite : Boolean) = {
    val payload = s"$metricName $measurement ${new Date().getTime / 1000}\n"
    logger.info(payload)
    if (sendToGraphite) {
      publishMetrics(payload)
    }
  }

  def publishMetrics(payload: String) = {
    import java.io.OutputStreamWriter
    val socket = new Socket(graphiteHost, graphitePort)
    try {
      val writer = new OutputStreamWriter(socket.getOutputStream)
      writer.write(payload)
      writer.flush()
      writer.close()
    } catch {
      case t:Throwable => logger.error("Error when publishing metrics", t)
    } finally
      socket.close
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2 && args.length <= 3, "Usage: ./searchmd-info.sh application.conf <bucketName> [loop=true|false]")

    val parsedConfig = ConfigFactory.parseFile(new File(args(0)))
    val _config = ConfigFactory.load(parsedConfig)

    val config = new CluesoConfig(_config)
    config.setSparkUiPort(4052)
    val spark = SparkUtils.buildSparkSession(config)
      .master("local[*]")
      .appName("")
      .getOrCreate()

    val fs = SparkUtils.buildHadoopFs(config)

    val bucketName = args(1)
    val loop = if (args.length == 3) args(2).toBoolean else false

    val landing_path = s"s3a://${config.bucketName}/landing/bucket=$bucketName"
    val staging_path = s"s3a://${config.bucketName}/staging/bucket=$bucketName"

    do {
      val (landingFileCount, landingAvgFileSize) = SparkUtils.getParquetFilesStats(fs, landing_path)
      val (stagingFileCount, stagingAvgFileSize) = SparkUtils.getParquetFilesStats(fs, staging_path)

      val landingRecordCount = MetadataQueryExecutor.getColdLandingTable(spark, config, bucketName).count()
      val stagingRecordCount = MetadataQueryExecutor.getColdStagingTable(spark, config, bucketName).count()

      // print metrics
      processMetrics(s"search_metadata.landing.$bucketName.parquet_file_count", landingFileCount, sendToGraphite = loop)
      processMetrics(s"search_metadata.staging.$bucketName.parquet_file_count", stagingFileCount, sendToGraphite = loop)

      processMetrics(s"search_metadata.landing.$bucketName.avg_file_size", landingAvgFileSize, sendToGraphite = loop)
      processMetrics(s"search_metadata.staging.$bucketName.avg_file_size", stagingAvgFileSize, sendToGraphite = loop)

      processMetrics(s"search_metadata.landing.$bucketName.total_file_size", landingAvgFileSize * landingFileCount, sendToGraphite = loop)
      processMetrics(s"search_metadata.staging.$bucketName.total_file_size", stagingAvgFileSize * stagingFileCount, sendToGraphite = loop)

      processMetrics(s"search_metadata.landing.$bucketName.record_count", landingRecordCount, sendToGraphite = loop)
      processMetrics(s"search_metadata.staging.$bucketName.record_count", stagingRecordCount, sendToGraphite = loop)

      Thread.sleep(3000) // measurement interval 3 sec
    } while (loop)
  }
}
