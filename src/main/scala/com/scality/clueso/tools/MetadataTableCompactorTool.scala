package com.scality.clueso.tools

import java.io.File

import com.scality.clueso.compact.TableFilesCompactor
import com.scality.clueso.{CluesoConfig, SparkUtils}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

object MetadataTableCompactorTool extends LazyLogging {

  def main(args: Array[String]): Unit = {
    require(args.length >= 3 && args.length <= 5, "Usage: ./table-compactor.sh <path/to/application.conf> <spark.master> <numPartitions> [<bucket>] [<forceCompaction>]")

    val sparkMaster = args(1)
    val numPartitions = args(2).toInt
    val bucket = if (args.length > 3)  Some(args(3))   else None
    val forceCompaction = if (args.length > 4)  args(4).toBoolean else false

    val parsedConfig = ConfigFactory.parseFile(new File(args(0)))
    val _config = ConfigFactory.load(parsedConfig)

    val config = new CluesoConfig(_config)
    config.setSparkUiPort(4053)

    import java.nio.file.Paths
    val currentJarPath = Paths.get(classOf[TableFilesCompactor].getProtectionDomain.getCodeSource.getLocation.toURI).toString

    logger.info(s"current JAR path = $currentJarPath")

    val spark = SparkUtils.buildSparkSession(config)
      .master(sparkMaster)
      .config("spark.executor.extraClassPath", currentJarPath)
      .appName("Table Files Compactor")
      .getOrCreate()


    val merger = new TableFilesCompactor(spark, config)

    if (bucket.isDefined) {
      merger.compactLandingPartition("bucket", bucket.get, numPartitions, forceCompaction)
    } else {
      merger.compact(numPartitions, forceCompaction)
    }
  }
}

