package com.scality.clueso.query.cache

import java.util.concurrent.atomic.AtomicReference

import com.scality.clueso.{CluesoConfig, PathUtils}
import com.scality.clueso.query.MetadataQueryExecutor.setupDf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime


object SessionCacheManager extends LazyLogging {
  var bucketDfs = Map[String, AtomicReference[DataFrame]]()
  var bucketLandingUpdateTs = Map[String, DateTime]()
  var bucketStagingUpdateTs = Map[String, DateTime]()

  val setupDfLock = new scala.collection.parallel.mutable.ParHashSet[String]()

  def getCachedBucketDataframe(spark: SparkSession, bucketName: String)(implicit config: CluesoConfig): DataFrame = {
    val tableView = s"${bucketName.replace("-","_").replace(".","__")}"

    // check if cache doesn't exist
    if (!bucketDfs.contains(bucketName)) {
      val bucketDf = setupDf(spark, config, bucketName)

      if (acquireLock(bucketName)) {
        logger.info(s"Cache does not exist. Creating tableView: $tableView")
        bucketDf.createOrReplaceTempView(tableView)
        bucketDf.sparkSession.catalog.cacheTable(tableView)

        bucketDfs = bucketDfs.updated(bucketName, new AtomicReference(bucketDf))
        bucketLandingUpdateTs = bucketLandingUpdateTs.updated(bucketName, DateTime.now())
        bucketStagingUpdateTs = bucketStagingUpdateTs.updated(bucketName, DateTime.now())
        releaseLock(bucketName)
      }
      logger.info(s"Cache does not exist. Created cache for $bucketName")
      bucketDf
    } else {
      logger.info(s"Cache exists for ${bucketName}")
      // cache exists
      if (bucketLandingUpdateTs(bucketName).plus(config.landingCacheExpiry).isBeforeNow ||
        bucketStagingUpdateTs(bucketName).plus(config.stagingCacheExpiry).isBeforeNow) {
        logger.info(s"Cache too old for ${bucketName}. Refreshing landing if can acquire lock.")
        // check if there's a dataframe update going on
        if (acquireLock(bucketName)) {
          logger.info(s"Acquired lock. Refreshing landing path ${PathUtils.landingURI}=${bucketName}/")
          // to remove spark metadata
          spark.catalog.refreshTable(tableView)
          spark.catalog.refreshByPath(s"/${PathUtils.landingURI}=${bucketName}/")
          bucketLandingUpdateTs = bucketLandingUpdateTs.updated(bucketName, DateTime.now())
          if (bucketStagingUpdateTs(bucketName).plus(config.stagingCacheExpiry).isBeforeNow) {
            logger.info(s"Also refreshing staging path ${PathUtils.stagingURI}=${bucketName}/")
            spark.catalog.refreshByPath(s"/${PathUtils.stagingURI}=${bucketName}/")
            bucketStagingUpdateTs = bucketStagingUpdateTs.updated(bucketName, DateTime.now())
          }
          releaseLock(bucketName) // unlock
        }
      }
      //  return most recent cached version (always)
      bucketDfs(bucketName).get()
    }
  }

  def acquireLock(bucketName : String) = synchronized {
    if (!setupDfLock.contains(bucketName)) {
      logger.info(s"Acquiring lock for bucket = ${bucketName}")
      setupDfLock += bucketName
      true
    } else
      false
  }

  def releaseLock(bucketName : String) = synchronized {
    logger.info(s"Releasing lock for bucket = ${bucketName}")
    setupDfLock -= bucketName
  }

}
