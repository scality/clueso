package com.scality.clueso.query.cache

import java.util.Date

import com.scality.clueso.AlluxioUtils._
import com.scality.clueso.CluesoConfig
import com.scality.clueso.query.MetadataQueryExecutor.setupDf
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object AlluxioCacheManager extends LazyLogging {
  def getCachedBucketDataframe(spark: SparkSession, bucketName: String, alluxioFs: FileSystem)(implicit config: CluesoConfig): DataFrame = {

    val cachePath: Option[Path] = getLatestCachePath(alluxioFs, bucketName)

    if (cachePath.isEmpty) {
      logger.info("Cache is empty")

      val bucketDf = setupDf(spark, config, bucketName)

      if (!cacheComputationBeingExecuted(alluxioFs, bucketName)) {
        // async exec:
        Future {
          val tmpDir = alluxioTempPath(Some(bucketName))
          logger.info(s"Cache being async written to ${tmpDir.toUri.toString}")

          bucketDf.write.parquet(tmpDir.toUri.toString)

          val tableView = alluxioCachePath(bucketName).toUri.toString

          logger.info(s"Renaming tmpDir ${tmpDir.toUri.toString} to ${tableView}")
          alluxioFs.rename(tmpDir, new Path(tableView))
        }
      }


      bucketDf
    } else {
      logger.info(s"Cache exists: ${cachePath.get}")

      // grab timestamp of _SUCCESS
      val fileStatus = alluxioFs.getFileStatus(new Path(cachePath.get, "_SUCCESS"))
      val now = new Date().getTime
      logger.info(s"Cache timestamp = ${fileStatus.getModificationTime} ( now = $now )  threshold = ${config.mergeFrequency.toMillis} ms")

      if (fileStatus.getModificationTime + config.mergeFrequency.toMillis < now) {
        // check if there's a dataframe update going on
        if (!cacheComputationBeingExecuted(alluxioFs, bucketName)) {

          val randomTempPath = alluxioTempPath(Some(bucketName)).toUri.toString

          // async update dataframe if there's none already doing it
          import scala.concurrent.ExecutionContext.Implicits.global
          Future {
            logger.info(s"Calculating view $randomTempPath")
            setupDf(spark, config, bucketName)
          } map { bucketDf =>
            logger.info(s"Calculating view $randomTempPath")

            // write it to Alluxio with a random viewName
            bucketDf.write.parquet(randomTempPath)

            val tableView = alluxioCachePath(bucketName).toUri.toString
            logger.info(s"Atomically swapping RDD for bucket = $bucketName ( new = tableView , old = $randomTempPath)")
            // once finished, swap atomically with existing bucket
            alluxioFs.rename(new Path(randomTempPath), new Path(tableView))

            // sleep before the cleanup
            Thread.sleep(config.cleanPastCacheDelay.toMillis) // TODO make configurable
            cleanPastCaches(alluxioFs, bucketName)
          }
        }
      }

      //  return most recent cached version (always)
      spark.read.parquet(cachePath.get.toUri.toString)
    }
  }
}
