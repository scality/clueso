package com.scality.clueso

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}

object AlluxioUtils extends LazyLogging {

  def deleteLockFile(alluxioFs: FileSystem, bucketName: String)(implicit config : CluesoConfig) = {
    alluxioFs.delete(alluxioLockPath(bucketName), false)
  }

  def acquireLock(alluxioFs: FileSystem, bucketName : String)(implicit config : CluesoConfig) = synchronized {
    // TODO race condition on alluxio?
    if (!alluxioFs.exists(alluxioLockPath(bucketName))) {
      alluxioFs.create(alluxioLockPath(bucketName), false)
      true
    } else
      false
  }

  def alluxioLockPath(bucketName : String)(implicit config : CluesoConfig) =
    new Path(s"${config.alluxioUrl}/lock_$bucketName")

  def alluxioCachePath(bucketName : String)(implicit config : CluesoConfig) =
    new Path(s"${config.alluxioUrl}/bucket_$bucketName")

  def alluxioTempPath(bucketName : Option[String])(implicit config : CluesoConfig) =
    new Path(s"${config.alluxioUrl}/tmp_${new Date().getTime}_${bucketName.getOrElse("")}")

  import java.util.regex.Pattern

  def cacheLocationRegex(bucketName: String): Pattern =
    Pattern.compile(s"(\\d+)_bucket_$bucketName")

  def cacheComputationLocationRegex(bucketName: String): Pattern =
    Pattern.compile(s"tmp_(\\d+)_$bucketName")

  def cacheComputationBeingExecuted(alluxioFs: FileSystem, bucketName : String)(implicit config : CluesoConfig) : Boolean = {
    val pattern = cacheComputationLocationRegex(bucketName)

    val status = alluxioFs.listStatus(new Path(config.alluxioUrl), new PathFilter {
      override def accept(path: Path): Boolean = pattern.matcher(path.getName).matches()
    })

    if (status.isEmpty) {
      false // no cache being computer
    } else {
      val latestCachePath = getLatestCachePath(alluxioFs, bucketName)

      // there are computations... pick up the max timestamp
      val maxTs = status.map { status =>
        val m = pattern.matcher(status.getPath.getName)
        (m.group(0).toLong, status.getPath)
      } maxBy(_._1)

      // get last cached view
      val cachePattern = cacheLocationRegex(bucketName)

      if (latestCachePath.isEmpty) {
        // cache being computer
        true
      } else {
        // check if cache is more recent than detected computation (may be pending for deletion)
        val cacheTs = cachePattern.matcher(latestCachePath.get.getName)
          .group(0)
          .toLong

        // is being computer if computation ts > most recent cache ts
        cacheTs < maxTs._1
      }
    }
  }

  def getCachePaths(alluxioFs: FileSystem, bucketName : String)(implicit config : CluesoConfig) = {
    val pattern = cacheLocationRegex(bucketName)

    alluxioFs.listStatus(new Path(config.alluxioUrl), new PathFilter {
      override def accept(path: Path): Boolean = pattern.matcher(path.getName).matches()
    })
  }

  def getLatestCachePath(alluxioFs: FileSystem, bucketName : String)(implicit config : CluesoConfig) = {
    val pattern = cacheLocationRegex(bucketName)

    val status = alluxioFs.listStatus(new Path(config.alluxioUrl), new PathFilter {
      override def accept(path: Path): Boolean = pattern.matcher(path.getName).matches()
    })

    if (status.isEmpty) {
      None
    } else {
      val maxTs = status.map { status =>
        val m = pattern.matcher(status.getPath.getName)
        (m.group(0).toLong, status.getPath)
      } maxBy(_._1)

      Some(maxTs._2)
    }
  }


  def cleanPastCaches(alluxioFs : FileSystem, bucketName : String)(implicit config : CluesoConfig) = {
    logger.info(s"Cleaning past caches")

    val paths = getCachePaths(alluxioFs, bucketName)

    if (paths.nonEmpty && paths.length > config.pastCacheRetention) {
      val pattern = cacheLocationRegex(bucketName)

      // pick oldest
      val oldest = paths.map { status =>
        val m = pattern.matcher(status.getPath.getName)
        (m.group(0).toLong, status.getPath)
      } maxBy(_._1)

      try {
        alluxioFs.delete(oldest._2, true)
        logger.info(s"Oldest cache for bucket $bucketName removed")
      } catch {
        case t: Throwable =>
          logger.warn(s"Failed removing oldest cache for bucket $bucketName")
      }
    }


//
  }

}
