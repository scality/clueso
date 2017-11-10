package com.scality.clueso.merge

import java.util.Date

import com.scality.clueso._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}

class TableFilesCompactor(spark : SparkSession, implicit val config: CluesoConfig) extends LazyLogging {
  val fs = SparkUtils.buildHadoopFs(config)
  val partDirnamePattern = "([A-Za-z0-9_]+)=(.*)".r
  val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd_hhmmss")

  def merge(numPartitions: Int, force: Boolean): Unit = {
    // go through all partitions and see which ones are eligible for merging
    val landingPartitionsIt = fs.listStatus(new Path(PathUtils.landingURI)).iterator

    while (landingPartitionsIt.hasNext) {
      val fileStatus = landingPartitionsIt.next()

      if (fileStatus.isDirectory &&
        fileStatus.getPath.getName.matches(partDirnamePattern.pattern.pattern())) {

        val regexMatch = partDirnamePattern.findFirstMatchIn(fileStatus.getPath.getName)

        regexMatch.foreach { rm =>
          compactLandingPartition(
            partitionColumnName = rm.group(1),
            partitionValue = rm.group(2),
            numPartitions, force)
        }
      }
    }
  }

  def deleteSparkMetadataDir(path: String): Unit = {
    logger.info(s"Ruthlessly deleting metadata dir on $path")

    val dirPath = new Path(path, "_spark_metadata")
    if (fs.exists(dirPath)) {
      fs.delete(dirPath, true)
    }
  }

  def getSubpartitionsToCompact(landingPartitionPath: String, force: Boolean): Array[String] = {
    val subpartitionPaths = fs.listStatus(new Path(landingPartitionPath), new PathFilter {
      override def accept(path: Path): Boolean = {
        fs.isDirectory(path) && path.getName.matches(partDirnamePattern.pattern.pattern())
      }
    })

    var subpartitionValues = subpartitionPaths
      .map(_.getPath.getName)
      .flatMap(subpartDirName => {
        val regexMatch = partDirnamePattern.findFirstMatchIn(subpartDirName)
        regexMatch.map { rm => rm.group(2) }
      })


    // force = true – all values
    // force = false
    //   subpartitions has 1 value -> List()
    //   subpartitions has 2+ values -> truncate the biggest one (assumes integer)
    if (!force) {
      subpartitionValues = if (subpartitionValues.length > 1) {
        subpartitionValues.map(_.toInt).sorted.dropRight(1).map(_.toString)
      } else {
        // only one subpartition exist, so no compaction will occur
        Array()
      }
    }

    subpartitionValues
  }

  def removeSubpartitionsFromLanding(landingPartitionPath: String, subPartToCompact: Array[String]): Unit = {
    val subpartitionPaths = fs.listStatus(new Path(landingPartitionPath), new PathFilter {
      override def accept(path: Path): Boolean = {
        if (fs.isDirectory(path) && path.getName.matches(partDirnamePattern.pattern.pattern())) {
          val regexMatch = partDirnamePattern.findFirstMatchIn(path.getName)
          regexMatch.foldLeft(false) { (v, rm) => v || subPartToCompact.contains(rm.group(2)) }
        } else {
          false
        }
      }
    })

    subpartitionPaths.foreach(fileStatus => fs.delete(fileStatus.getPath, true))
  }

  /**
    * Merges the contents of /landing/<partitionColumnName>=<partitionValue>/`*`/`*`.parquet and deletes
    * /landing/_spark_metadata. /landing/bucket=mybucketname/ will be composed of multiple subpartitions
    * with format maxOpIndex=<N>. If only of such subpartitions is existent, the partition bucket=.. won't be
    * merged into /staging. This can be forced by setting <force> to `true`
    *
    * @param partitionColumnName usually equal to 'bucket'
    * @param partitionValue      usually equal to the name of the bucket
    * @param numPartitions       num of partitions should be equal or multiple of the number of executors on Query applications
    * @param force               if true, merges even if there's only one  subpartition maxOpIndex=N
    */
  def compactLandingPartition(partitionColumnName: String, partitionValue: String, numPartitions: Int, force: Boolean) = {
    println(s"Merging partition $partitionColumnName=$partitionValue into $numPartitions files")

    val outputPath = s"${PathUtils.stagingURI}/$partitionColumnName=$partitionValue"

    val lockFilePath = lockPath()

    if (acquireLock(lockFilePath)) {
      val startTs = new Date().getTime

      val landingPartitionPath = s"${PathUtils.landingURI}/$partitionColumnName=$partitionValue"

      try {
        val subPartToCompact = getSubpartitionsToCompact(landingPartitionPath, force)

        if (subPartToCompact.nonEmpty) {

          val data = spark.read
            .schema(CluesoConstants.storedEventSchema)
            .parquet(landingPartitionPath)
            .where(col("maxOpIndex").isin(subPartToCompact))

          // window function over union of partitions bucketName=<specified bucketName>
          val windowSpec = Window.partitionBy("key").orderBy(col("kafkaTimestamp").desc)

          data.coalesce(numPartitions)
            .withColumn("rank", dense_rank().over(windowSpec))
            .where((col("rank") === 1).and(col("type") =!= "delete"))
            .drop("rank")
            .write
            .partitionBy("bucket", "maxOpIndex")
            .mode(SaveMode.Append)
            .parquet(outputPath)
        }

        logger.info(s"Waiting ${config.landingPurgeTolerance.toMillis}ms before purging compacted partitions")
        Thread.sleep(config.landingPurgeTolerance.toMillis)

        removeSubpartitionsFromLanding(landingPartitionPath, subPartToCompact)

        deleteSparkMetadataDir(PathUtils.landingURI)
      } catch {
        case e: Throwable =>
          logger.error("Thrown err from compactor", e)
      } finally {
        releaseLock(lockFilePath)
      }
    }
  }

  def lockPath() = s"${PathUtils.stagingURI}/_merging"

  def acquireLock(path: String) = fs.createNewFile(new Path(path))

  def releaseLock(path: String) = fs.delete(new Path(path), false)
}