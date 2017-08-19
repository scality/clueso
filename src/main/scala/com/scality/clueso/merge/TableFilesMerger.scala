package com.scality.clueso.merge

import java.io.File
import java.util.{Date, UUID}

import com.scality.clueso.SparkUtils.parquetFilesFilter
import com.scality.clueso.{CluesoConfig, CluesoConstants, SparkUtils}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class TableFilesMerger(spark : SparkSession, config: CluesoConfig) {

  val fs = SparkUtils.buildHadoopFs(config)


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
    val landingPartitionsIt = fs.listStatus(new Path(config.landingPath)).iterator

    while (landingPartitionsIt.hasNext) {
      val fileStatus = landingPartitionsIt.next()

      if (fileStatus.isDirectory &&
        fileStatus.getPath.getName.matches(partDirnamePattern.pattern.pattern())) {

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
//      .mode(SaveMode.Overwrite)
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
    println(s"Merging partition $partitionColumnName=$partitionValue into $numPartitions files")

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

