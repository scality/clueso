package com.scality.clueso

import java.io.{File, IOException}
import java.net.URI

import com.scality.clueso.query.{MetadataQuery, MetadataQueryExecutor}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.{Configuration => HadoopConfig}
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Row, SparkSession}


object SparkUtils extends LazyLogging {
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
    c.set("fs.s3a.path.style.access", config.s3PathStyleAccess)
    c.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem")

    c
  }

  def buildSparkSession(config : CluesoConfig) = {
    SparkSession
      .builder
      .config("spark.sql.parquet.cacheMetadata", "false")
      .config("spark.hadoop.fs.s3a.fast.upload", "true")
      .config("spark.hadoop.fs.s3a.buffer.dir", "./tmp")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
      .config("spark.hadoop.fs.s3a.endpoint", config.s3Endpoint)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", config.s3AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", config.s3SecretKey)
      .config("spark.hadoop.fs.s3a.path.style.access", config.s3PathStyleAccess)
      .config("spark.sql.streaming.metricsEnabled", "true")
      .config("fs.alluxio.impl", "alluxio.hadoop.FileSystem")

  }

  def confAlluxioCacheThruWrites(spark : SparkSession, config : CluesoConfig) = {
    logger.debug("Configuring Alluxio Client for CACHE_THROUGH writes")
    spark.conf.set("alluxio.user.file.writetype.default", "CACHE_THROUGH")
    spark.conf.set("alluxio.user.block.size.bytes.default", "16MB")
  }

  def confAlluxioCache(spark : SparkSession, config : CluesoConfig) = {
    logger.debug("Configuring Alluxio Client for MUST_CACHE writes")
    spark.conf.set("alluxio.user.file.writetype.default", "MUST_CACHE")
    spark.conf.set("alluxio.user.block.size.bytes.default", "16MB")
  }

  def confAlluxioReads(spark : SparkSession, config : CluesoConfig) = {
    logger.debug("Configuring Alluxio Client for CACHE_PROMOTE reads")
    spark.conf.set("alluxio.user.file.readtype.default", "CACHE_PROMOTE")
  }

  def confSparkSession(spark : SparkSession, config : CluesoConfig) = {
    spark.conf.set("spark.sql.parquet.cacheMetadata", "false")
    spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    spark.conf.set("spark.hadoop.fs.s3a.buffer.dir", "/tmp")
    spark.conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", config.s3Endpoint)
    spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.conf.set("spark.hadoop.fs.s3a.access.key", config.s3AccessKey)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", config.s3SecretKey)
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", config.s3PathStyleAccess)
    spark.conf.set("fs.s3a.path.style.access", config.s3PathStyleAccess)
    spark.conf.set("spark.sql.streaming.metricsEnabled", "true")
    spark.conf.set("fs.alluxio.impl", "alluxio.hadoop.FileSystem")

  }

  import scala.util.parsing.json.JSONObject

  def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString().replaceAll("`","")
  }

  def getQueryResults(spark : SparkSession, queryExecutor: MetadataQueryExecutor, query : MetadataQuery) = {
    val result = queryExecutor.execute(query)

    val resultArray = try {
//      val results = result.take(query.limit)

      val jsonResults = result.toJSON.take(query.limit)
      if (jsonResults.size > 0) {
        jsonResults.map(_.replaceAll("`",""))
      } else {
        Array[String]()
      }

//      if (results.nonEmpty) {
//        results.map(row => {
//          convertRowToJSON(row)
//        })
//      } else {
//        Array[String]()
//      }

    } catch {
      case e:IOException => {
        println(e)
        Array[String]()
      }
    }

    resultArray
  }


  val parquetFilesFilter = new PathFilter {
    override def accept(path: Path): Boolean = path.getName.endsWith(".parquet")
  }

  def fillNonExistingColumns(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x => x match {
      case x if myCols.contains(x) => col(x)
      case _ => lit(null).as(x)
    })
  }

  def getParquetFilesStats(fs: FileSystem, path : String) = {
    val fsPath = new Path(path)
    val statusList = if (!fs.exists(fsPath)) {
      Array[FileStatus]()
    } else {
      fs.listStatus(new Path(path), parquetFilesFilter)
    }

    val fileCount = statusList.count(_ => true)
    val avgFileSize = statusList.map(_.getLen).sum / Math.max(fileCount, 1)

    (fileCount, avgFileSize)
  }

  private[clueso] def createSet[T]() = {
    import scala.collection.JavaConverters._
    java.util.Collections.newSetFromMap(
      new java.util.concurrent.ConcurrentHashMap[T, java.lang.Boolean]).asScala
  }
}
