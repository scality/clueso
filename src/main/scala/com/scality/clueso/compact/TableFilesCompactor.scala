package com.scality.clueso.compact

import java.util.Date

import com.scality.clueso._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, dense_rank}

import scala.util.matching.Regex

class TableFilesCompactor(spark : SparkSession, implicit val config: CluesoConfig) extends LazyLogging {
  val fs = SparkUtils.buildHadoopFs(config)
  val partDirnamePattern = "([A-Za-z0-9_]+)=(.*)".r
  val dateFormat = new java.text.SimpleDateFormat("yyyyMMdd_hhmmss")


  /**
    * Performs a compaction on all existing buckets
    *
    * @param numPartitions
    * @param force if force = true, ignores the fact that some buckets may only have one subpartition maxOpIndex=N,
    *              which doesn't meet the compaction criteria (there has to be 2 or more). Compaction is performed
    *              regardless when force = true.
    */
  def compact(numPartitions: Int, force: Boolean): Unit = {
    val landingPartitionsIt = fs.listStatus(new Path(PathUtils.landingURI)).iterator

    while (landingPartitionsIt.hasNext) {
      val fileStatus = landingPartitionsIt.next()

      if (fileStatus.isDirectory &&
        fileStatus.getPath.getName.matches(partDirnamePattern.pattern.pattern())) {


        val regexMatches : Option[Regex.Match] = partDirnamePattern.findFirstMatchIn(fileStatus.getPath.getName)

        // only performs if there's a match
        regexMatches.foreach { rm =>
          compactLandingPartition(
            bucketColumn = rm.group(1), // "bucket"
            bucketNameValue = rm.group(2), // bucket name
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

  /**
    * Returns an array of Longs with all the  maxOpIndex subpartition values to compact
    *
    * @param landingPartitionPath path to scan
    * @param force
    * @return
    */
  def getSubpartitionsToCompact(landingPartitionPath: String, force: Boolean): Array[String] = {
    // subpartitionPaths contains all dirs that follows the pattern <partitionColumn>=<partitionValue>
    val subpartitionPaths = fs.listStatus(new Path(landingPartitionPath), new PathFilter {
      override def accept(path: Path): Boolean = {
        fs.isDirectory(path) && path.getName.matches(partDirnamePattern.pattern.pattern())
      }
    })

    // convert all subpartition values to Long
    var subpartitionValues = subpartitionPaths
      .map(_.getPath.getName)
      .flatMap(subpartDirName => {
        val regexMatch = partDirnamePattern.findFirstMatchIn(subpartDirName)
        regexMatch.map { rm => rm.group(2) }
      })
      .map(_.toLong)


    // force = true – all values
    // force = false
    //   subpartitions has 1 value -> Array()
    //   subpartitions has 2+ values -> drop the one with the highest maxOpIndex (the most recent)
    if (!force) {
      subpartitionValues = if (subpartitionValues.length > 1) {
        subpartitionValues.sorted.dropRight(1)
      } else {
        // only one subpartition exist, so no compaction will occur
        Array()
      }
    }

    subpartitionValues.map(_.toString)
  }

  /**
    * Delete's all the subpartitions with a value in <subPartToCompact> list
    *
    * @param landingPartitionPath path to a bucket partition
    * @param subPartToRemove list of maxOpIndex to remove
    */
  def removeSubpartitionsFromLanding(landingPartitionPath: String, subPartToRemove: Array[String]): Unit = {
    logger.info(s"Removing subpartitions from landing: ${landingPartitionPath}")
    val subpartitionPaths = fs.listStatus(new Path(landingPartitionPath), new PathFilter {
      override def accept(path: Path): Boolean = {
        // selects all the directories that follow the pattern K=V and with V present in   subPartToCompact   param

        if (fs.isDirectory(path) && path.getName.matches(partDirnamePattern.pattern.pattern())) {
          val regexMatch : Option[Regex.Match] = partDirnamePattern.findFirstMatchIn(path.getName)

          // returns true if there's a match and subpartition value (V) is present in subPartToCompact
          // rm.group(2) is the value of maxOpIndex
          regexMatch.foldLeft(false) { (v, rm) => v || subPartToRemove.contains(rm.group(2)) }
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
    * with format maxOpIndex=<N>.
    * If only one such subpartition exists, the partition bucket=.. won't be compacted into /staging.
    * However, compaction behaviour can be forced by setting <force> to `true`.
    *
    * @param bucketColumn         equal to 'bucket'
    * @param bucketNameValue      usually equal to the name of the bucket
    * @param numPartitions       num of partitions should be equal or multiple of the number of executors on Query applications
    * @param force               if true, merges even if there's only one  subpartition maxOpIndex=N
    */
  def compactLandingPartition(bucketColumn: String, bucketNameValue: String, numPartitions: Int, force: Boolean) = {
    logger.info(s"Merging partition $bucketColumn=$bucketNameValue into $numPartitions files")

    val lockFilePath = lockPath()

    if (acquireLock(lockFilePath)) {
      logger.info("Compaction Lock acquired")
      val startTs = new Date().getTime

      val landingPartitionPath = s"${PathUtils.landingURI}/$bucketColumn=$bucketNameValue"

      try {
        val subPartToCompact = getSubpartitionsToCompact(landingPartitionPath, force)

        if (subPartToCompact.nonEmpty) {
          logger.info(s"Number of subpartitions to compact = ${subPartToCompact.length}")

          var data = spark.read
            .schema(CluesoConstants.storedEventSchema)
            .parquet(s"${PathUtils.landingURI(config)}/$bucketColumn=$bucketNameValue/")
            .where(col("maxOpIndex").isin(subPartToCompact: _*))

          // window function over union of partitions bucketName=<specified bucketName>
          val windowSpec = Window.partitionBy("key").orderBy(col("opIndex").desc)

          data.coalesce(numPartitions)
            .withColumn("rank", dense_rank().over(windowSpec))
            .where((col("rank") === 1).and(col("type") =!= "delete"))
            .drop("rank")
            .write
            .partitionBy("maxOpIndex")
            .mode(SaveMode.Append)
            .parquet(s"${PathUtils.stagingURI(config)}/$bucketColumn=$bucketNameValue")

          logger.info(s"Successfully compacted bucket=$bucketNameValue")

          logger.info(s"Waiting ${config.landingPurgeTolerance}ms before purging compacted partitions")
          Thread.sleep(config.landingPurgeTolerance)
          removeSubpartitionsFromLanding(landingPartitionPath, subPartToCompact)
        } else {
          logger.info("No subpartitions to compact.")
        }

        deleteSparkMetadataDir(PathUtils.landingURI)
      } catch {
        case e: Throwable =>
          logger.error("Thrown err from compactor", e)
      } finally {
        releaseLock(lockFilePath)
      }
    } else {
      logger.error(s"Lock file detected, another compaction must be running. To force, delete $lockFilePath")
    }
  }

  def lockPath() = s"${PathUtils.stagingURI}/_merging"

  def acquireLock(path: String) = fs.createNewFile(new Path(path))

  def releaseLock(path: String) = fs.delete(new Path(path), false)
}