package com.scality.clueso

import java.io.File

import com.amazonaws.services.s3.S3ClientOptions
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider
import org.apache.spark.sql.SparkSession
import org.scalatest.Assertions

trait SparkContextSetup extends LazyLogging {
  def createBucketIfNotExists(config: CluesoConfig) = {
    import com.amazonaws.services.s3.AmazonS3Client
    val s3Client = new AmazonS3Client(new BasicAWSCredentialsProvider(config.s3AccessKey, config.s3SecretKey))

    try {
      val s3ClientOptions = new S3ClientOptions()
      s3ClientOptions.setPathStyleAccess(config.s3PathStyleAccess.toBoolean)
      s3Client.setS3ClientOptions(s3ClientOptions)
      s3Client.setEndpoint(config.s3Endpoint)


      if (!s3Client.doesBucketExist(config.bucketName)) {
        s3Client.createBucket(config.bucketName)
        logger.info(s"Creating bucket ${config.bucketName}")
      } else {
        logger.info(s"Bucket ${config.bucketName} exists")
      }
    } catch {
      case t : Throwable =>
        logger.error(s"Error while attempting to create bucket ${config.bucketName}", t)
    }

  }


def withSparkContext(testMethod: (SparkSession, CluesoConfig) => Any) {
    val parsedConfig = ConfigFactory.parseFile(new File(getClass.getResource("/application.conf").getFile))
    val _config = ConfigFactory.load(parsedConfig)

    val config = new CluesoConfig(_config)

    // create bucket if doesn't exist – this requires AWS client
    createBucketIfNotExists(config)

    val spark = SparkUtils.buildSparkSession(config)
      .master("local[*]")
      .appName("Integration Test " + getClass.toString)
      .getOrCreate()

    // create dirs
    val fs = SparkUtils.buildHadoopFs(config)

    val bucketPath = new Path(s"s3a://${config.bucketName}")
    if (!fs.exists(bucketPath)) {
      fs.create(bucketPath, true)
    }

    fs.delete(new Path(config.stagingPathUri), true)
    fs.delete(new Path(config.landingPathUri), true)

    try
      testMethod(spark, config)
    catch {
      case e: Exception =>
        e.printStackTrace()
        Assertions.fail(e)
    }
    finally {
      spark.stop()

      fs.delete(new Path(config.stagingPathUri), true)
      fs.delete(new Path(config.landingPathUri), true)
    }


  }
}
