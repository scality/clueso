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
    FileSystem.get(new URI(PathUtils.landingURI(config)), hadoopConfig(config))
  }

  def hadoopConfig(config: CluesoConfig): HadoopConfig = {
    val c = new HadoopConfig()
    c.set("fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
    c.set("fs.s3a.endpoint", config.s3Endpoint)
    c.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    c.set("fs.s3a.access.key", config.s3AccessKey)
    c.set("fs.s3a.secret.key", config.s3SecretKey)
    c.set("fs.s3a.path.style.access", config.s3PathStyleAccess)
    c.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.ui.port", config.sparkUiPort)
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
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }

  import scala.util.parsing.json.JSONObject

  def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString().replaceAll("`","")
  }

  def getQueryResults(spark : SparkSession, queryExecutor: MetadataQueryExecutor, query : MetadataQuery) = {
    val result = queryExecutor.execute(query)

    val resultArray = try {
      val jsonResults = result.toJSON.take(query.limit)
      if (jsonResults.size > 0) {
        jsonResults.map(_.replaceAll("`",""))
      } else {
        Array[String]()
      }
    } catch {
      case e:IOException => {
        logger.error("Error while computing search", e)
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

  def countParquetFiles(fs: FileSystem, fsPath : Path, extractFun : FileStatus => Long) : Long = {
    if (!fs.exists(fsPath)) {
      0L
    } else {
      val items = fs.listStatus(fsPath)

      var result = 0L

      for (item <- items) {
        if (item.isDirectory) {
          result += countParquetFiles(fs, item.getPath, extractFun)
        } else {
          if (parquetFilesFilter.accept(item.getPath)) {
            result += extractFun(item)
          }
        }
      }

      result
    }
  }

  def getParquetFilesStats(fs: FileSystem, path : String) = {
    val fileCount = countParquetFiles(fs, new Path(path), _ => 1)
    val avgFileSize = countParquetFiles(fs, new Path(path), stat => stat.getLen) / Math.max(fileCount, 1)

    (fileCount, avgFileSize)
  }
}
