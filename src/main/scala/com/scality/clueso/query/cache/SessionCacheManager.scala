package com.scality.clueso.query.cache

import java.util.Date
import java.util.concurrent.atomic.AtomicReference

import com.scality.clueso.CluesoConfig
import com.scality.clueso.query.MetadataQueryExecutor.setupDf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}

object SessionCacheManager extends LazyLogging {
  var bucketDfs = Map[String, AtomicReference[DataFrame]]()
  var bucketUpdateTs = Map[String, DateTime]()

  val setupDfLock = new scala.collection.parallel.mutable.ParHashSet[String]()

  def getCachedBucketDataframe(spark: SparkSession, bucketName: String)(implicit config: CluesoConfig): DataFrame = {
    val tableView = s"${new Date().getTime}_${bucketName.replaceAll("-","_").replaceAll("\\.","__")}"

    // check if cache doesn't exist
    if (!bucketDfs.contains(bucketName)) {
      val bucketDf = setupDf(spark, config, bucketName)

      if (acquireLock(bucketName)) {
        bucketDf.createOrReplaceTempView(tableView)
        bucketDf.sparkSession.catalog.cacheTable(tableView)

        bucketDfs = bucketDfs.updated(bucketName, new AtomicReference(bucketDf))
        bucketUpdateTs = bucketUpdateTs.updated(bucketName, DateTime.now())
        releaseLock(bucketName)
      }

      bucketDf
    } else {
      // cache exists
      if (bucketUpdateTs(bucketName).plus(config.cacheExpiry.toMillis).isBeforeNow) {
        // check if there's a dataframe update going on
        if (acquireLock(bucketName)) {

          // async update dataframe if there's none already doing it
          Future {
            logger.info(s"Calculating view $tableView")
            val bucketDf = setupDf(spark, config, bucketName, bucketDfs(bucketName).get())

            bucketDf.createOrReplaceTempView(tableView)

            // this operation triggers execution
            val cachingAsync = Future {
              bucketDf.sparkSession.catalog.cacheTable(tableView)
            }
            import scala.concurrent.duration._
            Await.ready(cachingAsync, 3 minutes)

            bucketDf
          } map { bucketDf =>
            logger.info(s"Calculating view $tableView")
            bucketDf.count() // force calculation
            logger.info(s"Atomically swapping DF for bucket = $bucketName ( new = $tableView )")
            val oldDf = bucketDfs.getOrElse(bucketName, new AtomicReference()).getAndSet(bucketDf)
            bucketUpdateTs = bucketUpdateTs.updated(bucketName, DateTime.now())
            releaseLock(bucketName) // unlock

            // sleep before deleting oldDf
            Thread.sleep(config.cleanPastCacheDelay.toMillis)
            logger.info(s"Unpersisting ${oldDf.rdd.name} after 10sec")
            oldDf.unpersist(true)
          }
        }

        // set the time as updated, to avoid triggering it again
        bucketUpdateTs = bucketUpdateTs.updated(bucketName, DateTime.now())
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
