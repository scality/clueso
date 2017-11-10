package com.scality.clueso.tools

import java.io.File

import com.scality.clueso.merge.TableFilesCompactor
import com.scality.clueso.{CluesoConfig, SparkUtils}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.rogach.scallop._

object MetadataTableMergerTool extends LazyLogging {

  class ToolConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val applicationConfFile = trailArg[String](required = true, descr = "application configuration file")
    val numPartitions = trailArg[Int](required = true)
    val bucket = trailArg[String](required = false)
    verify()
  }

  def main(args: Array[String]): Unit = {
    runTool(new ToolConf(args))
  }

  def runTool(toolConfig: ToolConf) = {
    val parsedConfig = ConfigFactory.parseFile(new File(toolConfig.applicationConfFile.getOrElse("")))
    val _config = ConfigFactory.load(parsedConfig)

    val config = new CluesoConfig(_config)

    val spark = SparkUtils.buildSparkSession(config)
      .master("local[*]")
      .appName("Table Files Merger")
      .getOrCreate()


    val merger = new TableFilesCompactor(spark, config)

    if (toolConfig.bucket.supplied) {
      merger.compactLandingPartition("bucket", toolConfig.bucket.apply(), toolConfig.numPartitions.apply(), false)
    } else {
      merger.merge(toolConfig.numPartitions.apply(), false)
    }
  }
}
