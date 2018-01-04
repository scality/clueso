package com.scality.clueso.query.cache

import java.util.Date
import java.util.concurrent.atomic.AtomicReference

import com.scality.clueso.CluesoConfig
import com.scality.clueso.query.MetadataQueryExecutor.setupDf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object SessionCacheManager extends LazyLogging {
  var bucketDfs = Map[String, AtomicReference[DataFrame]]()
  var bucketUpdateTs = Map[String, DateTime]()

  val setupDfLock = new scala.collection.parallel.mutable.ParHashSet[String]()

  def getCachedBucketDataframe(spark: SparkSession, bucketName: String)(implicit config: CluesoConfig): DataFrame = {
    val tableView = s"${new Date().getTime}_${bucketName.replace("-","_").replace(".","__")}"

    // check if cache doesn't exist
    if (!bucketDfs.contains(bucketName)) {
      val bucketDf = setupDf(spark, config, bucketName)

      if (acquireLock(bucketName)) {
        logger.info(s"Cache does not exist. Creating tableView: $tableView")
        bucketDf.createOrReplaceTempView(tableView)
        bucketDf.sparkSession.catalog.cacheTable(tableView)

        bucketDfs = bucketDfs.updated(bucketName, new AtomicReference(bucketDf))
        bucketUpdateTs = bucketUpdateTs.updated(bucketName, DateTime.now())
        releaseLock(bucketName)
      }
      logger.info(s"Cache does not exist. Created cache for $bucketName")
      bucketDf
    } else {
      logger.info(s"Cache exists for ${bucketName}")
      // cache exists
      if (bucketUpdateTs(bucketName).plus(config.cacheExpiry.toMillis).isBeforeNow) {
        logger.info(s"Cache too old for ${bucketName}. Recalculating if can acquire lock.")
        // check if there's a dataframe update going on
        if (acquireLock(bucketName)) {
          logger.info(s"Acquired lock. Calculating view $tableView")
          // calculating view by recalculating staging as well (otherwise, we will constantly accumulate cache based on prior)
          val bucketDf = setupDf(spark, config, bucketName)
          bucketDf.createOrReplaceTempView(tableView)

          // this operation triggers execution
          bucketDf.sparkSession.catalog.cacheTable(tableView)
          // to remove spark metadata
          spark.catalog.refreshTable(tableView)
          logger.info(s"Atomically swapping DF for bucket = $bucketName ( new = $tableView )")
          val oldDf = bucketDfs.getOrElse(bucketName, new AtomicReference()).getAndSet(bucketDf)
          bucketUpdateTs = bucketUpdateTs.updated(bucketName, DateTime.now())
          releaseLock(bucketName) // unlock

          Future {
            logger.info(s"Async calling sleep to unpersist old df for ${bucketName}")
            // sleep before deleting oldDf
            val sleepDuration = config.cleanPastCacheDelay.toMillis
            logger.info(s"Sleeping cleanPastCacheDelay = $sleepDuration ms")
            Thread.sleep(sleepDuration)
            logger.info(s"Unpersisting ${oldDf.rdd.id} for $bucketName after having slept $sleepDuration ms")
            oldDf.unpersist(true)
          }
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
