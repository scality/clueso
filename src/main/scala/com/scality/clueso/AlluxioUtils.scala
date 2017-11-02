package com.scality.clueso

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

object AlluxioUtils extends LazyLogging {

  def alluxioCachePath(bucketName: String)(implicit config: CluesoConfig) =
    new Path(s"${config.alluxioUrl}/${new Date().getTime}_bucket_$bucketName")

  def alluxioTempPath(bucketName: Option[String])(implicit config: CluesoConfig) =
    new Path(s"${config.alluxioUrl}/tmp_${new Date().getTime}_${bucketName.getOrElse("")}")

  import java.util.regex.Pattern

  def cacheLocationRegex(bucketName: String): Pattern =
    Pattern.compile(s"(\\d+)_bucket_$bucketName")

  def cacheComputationLocationRegex(bucketName: String): Pattern =
    Pattern.compile(s"tmp_(\\d+)_$bucketName")

  def cacheComputationBeingExecuted(alluxioFs: FileSystem, bucketName: String)(implicit config: CluesoConfig): Boolean = {
    val pattern = cacheComputationLocationRegex(bucketName)
    val status = alluxioFs.listStatus(new Path(config.alluxioUrl), new PathFilter {
      override def accept(path: Path): Boolean = pattern.matcher(path.getName).matches()
    })

    if (status.isEmpty) {
      logger.info("No cache being computed.")
      false // no cache being computed
    } else {
      logger.info("Cache being computed...")
      val latestCachePath = getLatestCachePath(alluxioFs, bucketName)
      // there are computations... pick up the max timestamp
      val maxTs = status.flatMap { status =>
        val m = pattern.matcher(status.getPath.getName)

        if (m.matches()) {
          Some((m.group(0).toLong, status.getPath))
        } else {
          None
        }

      } maxBy (_._1)
      // get last cached view
      val cachePattern = cacheLocationRegex(bucketName)
      if (latestCachePath.isEmpty) {
        // cache being computed
        logger.info("Cache being computed by other Query")
        true
      } else {
        // check if cache is more recent than detected computation (may be pending for deletion)
        val cacheTs = cachePattern.matcher(latestCachePath.get.getName)
          .group(0)
          .toLong
        // is being computer if computation ts > most recent cache ts
        logger.info("Comparing cache timestamps.")
        cacheTs < maxTs._1
      }
    }
  }

  def getCachePaths(alluxioFs: FileSystem, bucketName: String)(implicit config: CluesoConfig) = {
    val pattern = cacheLocationRegex(bucketName)
    alluxioFs.listStatus(new Path(config.alluxioUrl), new PathFilter {
      override def accept(path: Path): Boolean = pattern.matcher(path.getName).matches()
    })
  }

  def getLatestCachePath(alluxioFs: FileSystem, bucketName: String)(implicit config: CluesoConfig) = {
    val pattern = cacheLocationRegex(bucketName)
    val status = alluxioFs.listStatus(new Path(config.alluxioUrl), new PathFilter {
      override def accept(path: Path): Boolean = pattern.matcher(path.getName).matches()
    })
    if (status.isEmpty) {
      None
    } else {
      val maxTs = status.flatMap { status =>
        logger.debug(s"Detected cache = ${status.getPath.getName}")
        val m = pattern.matcher(status.getPath.getName)

        if (m.matches()) {
          Some((m.group(1), status.getPath))
        } else {
          None
        }
      } maxBy (_._1)

      logger.debug(s"Last cache = ${maxTs._2}")
      Some(maxTs._2)
    }
  }

  def cleanPastCaches(alluxioFs: FileSystem, bucketName: String)(implicit config: CluesoConfig) = {
    logger.info(s"Cleaning past caches")
    val paths = getCachePaths(alluxioFs, bucketName)
    if (paths.nonEmpty && paths.length > config.pastCacheRetention) {
      val pattern = cacheLocationRegex(bucketName)
      // pick oldest
      val oldest = paths.map { status =>
        val m = pattern.matcher(status.getPath.getName)
        (m.group(0).toLong, status.getPath)
      } maxBy (_._1)
      try {
        alluxioFs.delete(oldest._2, true)
        logger.info(s"Oldest cache for bucket $bucketName removed")
      } catch {
        case t: Throwable =>
          logger.warn(s"Failed removing oldest cache for bucket $bucketName")
      }
    }
  }

  // alluxio settings set out here: http://www.alluxio.org/docs/master/en/Configuration-Properties.html
  // appears default cache is 1 hour? change to 60 seconds to match merge frequency?
  def landingURI(implicit config: CluesoConfig): String = {
    if (!config.useAlluxio) {
      config.landingPath
    } else s"${config.alluxioUrl}${config.alluxioLandingPath}"
  }

  def stagingURI(implicit config: CluesoConfig): String = {
    if (!config.useAlluxio) {
      config.stagingPath
    } else s"${config.alluxioUrl}${config.alluxioStagingPath}"
  }

}
