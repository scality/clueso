package com.scality.clueso.tools

import java.io.File
import java.net.URI

import com.scality.clueso.SparkUtils.hadoopConfig
import com.scality.clueso.{AlluxioUtils, CluesoConfig}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.FileSystem
import org.rogach.scallop.ScallopConf

object MetadataCacheTool  extends LazyLogging {

  class ToolConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val applicationConfFile = trailArg[String](required = true, descr = "application configuration file")
    val operation = trailArg[String]()
    val bucket = trailArg[String]()
    verify()
  }

  def main(args: Array[String]): Unit = {
    runTool(new ToolConf(args))
  }

  def printUsage(): Unit = {
    System.err.println("Usage: ./cache-tool.sh <application.conf> <operation> <op args>\n" +
      " Operations:\n" +
      "  - lockdel <bucketName> – Deletes the lock files for /bucketName/ cache computation"
    )
  }


  def runTool(toolConfig: ToolConf) = {
    val parsedConfig = ConfigFactory.parseFile(new File(toolConfig.applicationConfFile.getOrElse("")))
    val _config = ConfigFactory.load(parsedConfig)

    implicit val config = new CluesoConfig(_config)

    val alluxioFs = FileSystem.get(new URI(s"${config.alluxioUrl}/"), hadoopConfig(config))

    if (toolConfig.operation.getOrElse("").equals("lockdel") &&
      toolConfig.bucket.getOrElse("").nonEmpty) {

      AlluxioUtils.deleteLockFile(alluxioFs, toolConfig.bucket.getOrElse(""))
    } else {
      printUsage()
    }
  }
}