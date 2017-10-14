package com.scality.clueso

import com.typesafe.config.Config

class CluesoConfig(config: Config) {
  val s3SslEnabled = config.getString("s3_ssl_enabled")
  val s3Endpoint = config.getString("s3_endpoint")
  val s3AccessKey = config.getString("s3_access_key")
  val s3SecretKey = config.getString("s3_secret_key")
  val s3PathStyleAccess = config.getString("s3_path_style_access")
  val checkpointPath = config.getString("checkpoint_path")

  // locations
  val bucketName = config.getString("bucket_name")
  val bucketLandingPath = config.getString("bucket_landing_path")
  val bucketStagingPath = config.getString("bucket_staging_path")
  val bucketMergePath = config.getString("bucket_merge_path")

  val checkpointUrl = s"s3a//$bucketName$checkpointPath"

  val landingPath = s"s3a://$bucketName$bucketLandingPath"
  val stagingPath = s"s3a://$bucketName$bucketStagingPath"
  val mergePath = s"s3a://$bucketName$bucketMergePath"

  val alluxioLandingPath = s"s3a://$bucketName$bucketLandingPath"
  val alluxioStagingPath = s"s3a://$bucketName$bucketStagingPath"

  val mergeFrequency = config.getDuration("merge_frequency")
  val mergeFactor = config.getInt("merge_factor")
  val mergeMinFiles = config.getInt("merge_min_files")

  // as S3 object store is eventual consistency, this sets a minimum 'age' for landing files
  // to be deleted, avoiding data loss on merge operation
  val landingPurgeTolerance = config.getDuration("landing_purge_tolerance")


  val triggerTime = config.getDuration("trigger_time")

  val kafkaBootstrapServers = config.getString("kafka_bootstrap_servers")
  val kafkaTopic = config.getString("kafka_topic")

  val cacheDataframes = config.hasPath("cache_dataframes") && config.getBoolean("cache_dataframes")

  val graphiteHost = config.getString("graphite.hostname")
  val graphitePort = config.getInt("graphite.port")

  val alluxioHost = config.getString("alluxio.hostname")
  val alluxioPort = config.getInt("alluxio.port")


  def alluxioUrl = s"alluxio://$alluxioHost:$alluxioPort/"

  import scala.collection.JavaConversions._
  override def toString: String = config.entrySet().map { entry =>
      s"  ${entry.getKey} = ${entry.getValue.toString}"
    }.mkString("\n")
}
