package com.scality.clueso

import java.io.File
import java.net.URI

import com.scality.clueso.query.MetadataQuery
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.{Configuration => HadoopConfig}
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def loadCluesoConfig(confFilePath: String) = {
    val parsedConfig = ConfigFactory.parseFile(new File(confFilePath))
    val _config = ConfigFactory.load(parsedConfig)

    new CluesoConfig(_config)
  }

  def buildHadoopFs(config: CluesoConfig) = {
    FileSystem.get(new URI(config.mergePath), hadoopConfig(config))
  }

  def hadoopConfig(config: CluesoConfig): HadoopConfig = {
    val c = new HadoopConfig()
    c.set("fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
    c.set("fs.s3a.endpoint", config.s3Endpoint)
    c.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    c.set("fs.s3a.access.key", config.s3AccessKey)
    c.set("fs.s3a.secret.key", config.s3SecretKey)
    c
  }

  def buildSparkSession(config : CluesoConfig) = {
    SparkSession
      .builder
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
      .config("spark.hadoop.fs.s3a.endpoint", config.s3Endpoint)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", config.s3AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", config.s3SecretKey)
      .config("spark.hadoop.fs.s3a.path.style.access", config.s3PathStyleAccess)
  }

  def confSparkSession(spark : SparkSession, config : CluesoConfig) = {
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", config.s3Endpoint)
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", config.s3AccessKey)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", config.s3SecretKey)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", config.s3PathStyleAccess)
  }

  def getQueryResults(spark : SparkSession, query : MetadataQuery) = {
//    import scala.util.parsing.json.JSONObject
//
//    val results = ()
//
//    results
    query.execute.toJSON.collect
  }

  val parquetFilesFilter = new PathFilter {
    override def accept(path: Path): Boolean = path.getName.endsWith(".parquet")
  }
}
