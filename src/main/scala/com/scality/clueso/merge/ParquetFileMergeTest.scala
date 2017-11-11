package com.scality.clueso.merge

import com.scality.clueso.CluesoConfig
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object ParquetFileMergeTest {
  def main(args: Array[String]): Unit = {
    val config = new CluesoConfig(ConfigFactory.load)

    val spark = SparkSession
      .builder
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
      .config("spark.hadoop.fs.s3a.endpoint", config.s3Endpoint)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", config.s3AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", config.s3SecretKey)
      .master("local[*]")
      .appName("S3 Merge Test")
      .getOrCreate()


    ParquetFileMerger.merge(config, spark, "s3a://mybucket/output/bucket=gobig")
  }
}
