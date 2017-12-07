package com.scality.clueso.tools

import java.io.File

import com.scality.clueso.{CluesoConfig, PathUtils, SparkContextSetup, SparkUtils}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class LandingPopulatorSpec extends WordSpec with Matchers with SparkContextSetup {
  "Landing Populator" should {
    "Scenario 1: populate landing" in withSparkContext {
      (spark, config) =>
      val bucketName = "foobucket"
      val numberParquetFiles = "10"
      val configPath = getClass.getResource("/application.conf").toString.substring(5)
      LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "100", numberParquetFiles))
      val fs = SparkUtils.buildHadoopFs(config)
      fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
        SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt
    }
    }
}
