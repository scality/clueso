package com.scality.clueso.merge

import java.io.File
import java.net.URI
import java.util.{Date, UUID}

import com.scality.clueso.{CluesoConfig, CluesoConstants}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.{Configuration => HadoopConfig}
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

class TableFilesMerger(config: CluesoConfig) {
  val parquetFilesFilter = new PathFilter {
    override def accept(path: Path): Boolean = path.getName.endsWith(".parquet")
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

  val fs = FileSystem.get(new URI(config.mergePath), hadoopConfig(config))

  val spark = SparkSession
    .builder
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
    .config("spark.hadoop.fs.s3a.endpoint", config.s3Endpoint)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.access.key", config.s3AccessKey)
    .config("spark.hadoop.fs.s3a.secret.key", config.s3SecretKey)
    .master("local[*]")
    .appName("Table Files Merger")
    .getOrCreate()


  case class MergeInstructions(numPartitions : Int)

  def checkMergeEligibility(getPath: Path): Option[MergeInstructions] = {
    // number of records criteria
    val numberOfFiles = fs.listStatus(getPath, parquetFilesFilter).length

    if (numberOfFiles >= config.mergeMinFiles) {
      val data = spark.read
          .parquet(getPath.toUri.toString)

      val numRecords = data.count
      val numFinalFiles = Math.floor(numRecords / config.mergeFactor).toInt + 1

      if (numFinalFiles < numberOfFiles) {
        return Some(MergeInstructions(numFinalFiles))
      }
    }

    None
  }


  val partDirnamePattern = "([A-Za-z0-9_]+)=(.*)".r

  def merge() = {
    // go thru all partitions and see which ones are eligible for merging
    val landingPartitionsIt = fs.listStatusIterator(new Path(config.landingPath))

    while (landingPartitionsIt.hasNext) {
      val fileStatus = landingPartitionsIt.next()

      if (fileStatus.isDirectory &&
        fileStatus.getPath.getName.matches(partDirnamePattern.regex)) {

        checkMergeEligibility(fileStatus.getPath) match {
          case Some(MergeInstructions(numPartitions)) => {
            val regexMatch = partDirnamePattern.findFirstMatchIn(fileStatus.getPath.getName)

            regexMatch.map { rm =>
              mergePartition(partitionColumnName = rm.group(1),
                partitionValue = rm.group(2),
                numPartitions)
            }
          }
          case _ =>
        }


      }
    }
  }

  def writeMergedData(mergedData: Dataset[Row], outputPath: String) = {
    mergedData.write
      .parquet(outputPath)
  }

  def replaceStagingWithMerged(stagingPartitionPath: String, outputPath: String) = {
    // list all files in staging
    val stagingParquetFiles = fs.listStatus(new Path(stagingPartitionPath), parquetFilesFilter)

    // move all from merge path to staging
    val mergedFiles = fs.listStatus(new Path(outputPath), parquetFilesFilter)

    mergedFiles.foreach(mf => fs.rename(mf.getPath, new Path(s"$stagingPartitionPath/${mf.getPath.getName}")))

    // delete files that were on staging
    stagingParquetFiles.foreach(sf => fs.delete(sf.getPath, false))
  }

  def removeProcessedFromLanding(landingPartitionPath: String, startTs: Long) = {
    val landedParquetFiles = fs.listStatus(new Path(landingPartitionPath), parquetFilesFilter)
    val mergedLandingFiles = landedParquetFiles.filter(lf => lf.getModificationTime < startTs - config.landingPurgeTolerance.toMillis)

    mergedLandingFiles.foreach(lf => fs.delete(lf.getPath, false))
  }

  def mergePartition(partitionColumnName : String, partitionValue : String, numPartitions : Int) = {
    val mergeOutputId = UUID.randomUUID()
    val mergePath = s"${config.mergePath}/$mergeOutputId"
    val outputPath = s"$mergePath/merged_data"

    val lockFilePath = lockPath(mergePath)

    if (acquireLock(lockFilePath)) {
      val startTs = new Date().getTime

      val stagingPartitionPath = s"${config.stagingPath}/$partitionColumnName=$partitionValue"
      val landingPartitionPath = s"${config.landingPath}/$partitionColumnName=$partitionValue"

      try {
        val mergedData = readLandingAndStagingTableData(landingPartitionPath, stagingPartitionPath)

        writeMergedData(mergedData.coalesce(numPartitions), outputPath)

        replaceStagingWithMerged(stagingPartitionPath, outputPath)

        removeProcessedFromLanding(landingPartitionPath, startTs)
      } catch {
        case e: Throwable =>
          println(e)
      } finally {
        releaseLock(lockFilePath)
      }
    }
  }

  def lockPath(path : String) = s"$path/_merging"

  def acquireLock(path : String) = fs.createNewFile(new Path(path))

  def releaseLock(path : String) = fs.delete(new Path(path), false)

  def readLandingAndStagingTableData(landingPartPath : String, stagingPartPath : String) = {
    val landingData = spark.read
      .schema(CluesoConstants.sstSchema)
      .parquet(landingPartPath)

    val stagingPath = new Path(stagingPartPath)
    if (!fs.exists(stagingPath)) {
      fs.mkdirs(stagingPath)
      landingData
    } else {
      try {
        val stagingData = spark.read
          .schema(CluesoConstants.sstSchema)
          .parquet(stagingPartPath)

        // TODO distinct on event unique key
        landingData
          .union(stagingData)
          .distinct()

      } catch {
        case t:Throwable => {
          println(t)
          throw t
        }
      }
    }
  }
}

object TableFilesMerger {

  def main(args: Array[String]): Unit = {
    require(args.length > 0, "specify configuration file")

    val parsedConfig = ConfigFactory.parseFile(new File(args(0)))
    val _config = ConfigFactory.load(parsedConfig)

    val config = new CluesoConfig(_config)

    val merger = new TableFilesMerger(config)

    merger.merge()
  }
}

