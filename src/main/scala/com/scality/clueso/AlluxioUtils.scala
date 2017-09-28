package com.scality.clueso

import org.apache.hadoop.fs.{FileSystem, Path}

object AlluxioUtils {

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


  // TODO config alluxio master
  def alluxioCachePath(bucketName : String)(implicit config : CluesoConfig) =
    new Path(s"${config.alluxioUrl}/bucket_$bucketName")

  // TODO config alluxio master
  def alluxioTempPath(bucketName : Option[String])(implicit config : CluesoConfig) =
    new Path(s"${config.alluxioUrl}/tmp_${(Math.random()*10000).toLong}_${bucketName.getOrElse("")}")


}
