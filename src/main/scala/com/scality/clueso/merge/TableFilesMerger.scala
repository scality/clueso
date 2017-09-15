package com.scality.clueso.merge

import java.io.File
import java.util.{Date, UUID}

import com.scality.clueso.SparkUtils.parquetFilesFilter
import com.scality.clueso.query.MetadataQueryExecutor
import com.scality.clueso.{CluesoConfig, CluesoConstants, SparkUtils}
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class TableFilesMerger(spark : SparkSession, config: CluesoConfig) extends LazyLogging{

  val fs = SparkUtils.buildHadoopFs(config)

  val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd_hhmmss")

  case class MergeInstructions(numPartitions : Int)

  def checkMergeEligibility(bucketName : String): Option[MergeInstructions] = {
    val landing_path = s"${config.landingPath}/bucket=$bucketName"
    val staging_path = s"${config.stagingPath}/bucket=$bucketName"

    val (landingFileCount, landingAvgFileSize) = SparkUtils.getParquetFilesStats(fs, landing_path)
    val (stagingFileCount, stagingAvgFileSize) = SparkUtils.getParquetFilesStats(fs, staging_path)

    logger.info(s"Number of files in $landing_path: $landingFileCount")
    logger.info(s"Avg File Size (MB) in $landing_path: ${landingAvgFileSize/1024/1024}")

    logger.info(s"Number of files in $staging_path: $stagingFileCount")
    logger.info(s"Avg File Size (MB) in $staging_path: ${stagingAvgFileSize/1024/1024}")


    logger.info(s"Number of merge min files (configuration) : ${config.mergeMinFiles}")

    if (landingFileCount >= config.mergeMinFiles) {

      val landingRecordCount = MetadataQueryExecutor.getColdLandingTable(spark, config, bucketName).count()
      val stagingRecordCount = MetadataQueryExecutor.getColdStagingTable(spark, config, bucketName).count()

      logger.info(s"Merge Factor = ${config.mergeFactor}")
      var numFinalFiles = Math.floor((landingRecordCount + stagingRecordCount) / config.mergeFactor).toInt + 1
      numFinalFiles += numFinalFiles % 8

      logger.info(s"Number of records in landing = $landingRecordCount")
      logger.info(s"Number of records in staging = $stagingRecordCount")
      logger.info(s"Calculated number of final files in staging: $numFinalFiles")

      if (numFinalFiles < (landingFileCount)) {
        return Some(MergeInstructions(numFinalFiles))
      }
    }

    None
  }


  val partDirnamePattern = "([A-Za-z0-9_]+)=(.*)".r

  def merge() = {
    // go thru all partitions and see which ones are eligible for merging
    val landingPartitionsIt = fs.listStatus(new Path(config.landingPath)).iterator

    while (landingPartitionsIt.hasNext) {
      val fileStatus = landingPartitionsIt.next()

      if (fileStatus.isDirectory &&
        fileStatus.getPath.getName.matches(partDirnamePattern.pattern.pattern())) {

        val regexMatch = partDirnamePattern.findFirstMatchIn(fileStatus.getPath.getName)


        regexMatch.map { rm =>
          checkMergeEligibility(rm.group(2)) match {
            case Some(MergeInstructions(numPartitions)) => {
              mergePartition(partitionColumnName = rm.group(1),
                partitionValue = rm.group(2),
                numPartitions)
            }
            case _ =>
          }
        }
      }
    }
  }

  def writeMergedData(mergedData: Dataset[Row], outputPath: String) = {
    mergedData.write
//      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }

  def replaceStagingWithMerged(stagingPartitionPath: String, outputPath: String) = {
    logger.info("Replace Staging Files With Merged")
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

    logger.info("Removing processed files from Landing:\n  %s", mergedLandingFiles.map(_.getPath.getName).mkString("\n  "))

    mergedLandingFiles.foreach(lf => fs.delete(lf.getPath, false))
  }

  def deleteMetadataDir(landingPartitionPath: String): Unit = {
    logger.info("Ruthlessly deleting metadata dir")

    val dirPath = new Path(landingPartitionPath, "_spark_metadata")
    if (fs.exists(dirPath)) {
      fs.delete(dirPath, true)
    }
  }

  def mergePartition(partitionColumnName : String, partitionValue : String, numPartitions : Int) = {
    println(s"Merging partition $partitionColumnName=$partitionValue into $numPartitions files")

    val mergeOutputId =  UUID.randomUUID().toString.replaceAll("-","").substring(0, 5)
    val mergePath = s"${config.mergePath}/${dateFormat.format(new java.util.Date())}_$mergeOutputId"
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

        deleteMetadataDir(landingPartitionPath)

        // TODO wait a bit before removing..

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

  def readLandingAndStagingTableData(landingPartPath : String, stagingPartPath : String) : DataFrame = {
    val landingTable = spark.read
      .schema(CluesoConstants.storedEventSchema)
      .parquet(landingPartPath)

    val stagingPath = new Path(stagingPartPath)

    var union : Option[DataFrame] = None

    try {
      if (!fs.exists(stagingPath)) {
        fs.mkdirs(stagingPath)
        union = Some(landingTable)
      } else {
        val stagingTable = spark.read
          .schema(CluesoConstants.storedEventSchema)
          .parquet(stagingPartPath)

        val colsLanding = landingTable.columns.toSet
        val colsStaging = stagingTable.columns.toSet
        val unionCols = colsLanding ++ colsStaging

        import SparkUtils.fillNonExistingColumns

        union = Some(
          landingTable
            .select(fillNonExistingColumns(colsLanding, unionCols): _*)
            .union(
              stagingTable
                .select(fillNonExistingColumns(colsStaging, unionCols): _*))
            .toDF())
      }

      // window function over union of partitions bucketName=<specified bucketName>
      val windowSpec = Window.partitionBy("key").orderBy(col("kafkaTimestamp").desc)

      union.map(unionTable => {
        unionTable.orderBy(col("key"))
          .withColumn("rank", dense_rank().over(windowSpec))
          .where((col("rank") === 1).and(col("type") =!= "delete"))
          .drop("rank")
      }).getOrElse(spark.emptyDataFrame)

    } catch {
      case t:Throwable => {
        println(t)
        throw t
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

    val spark = SparkUtils.buildSparkSession(config)
      .master("local[*]")
      .appName("Table Files Merger")
      .getOrCreate()


    val merger = new TableFilesMerger(spark, config)

    merger.merge()
  }
}

