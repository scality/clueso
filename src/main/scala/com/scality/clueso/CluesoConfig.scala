package com.scality.clueso

import com.typesafe.config.Config

class CluesoConfig(config: Config) {
  // storage params
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

  val landingPathUri = s"s3a://$bucketName$bucketLandingPath"
  val stagingPathUri = s"s3a://$bucketName$bucketStagingPath"
  val checkpointUrl = s"s3a//$bucketName$checkpointPath"

  // compaction
  //    as S3 object store is eventual consistency, this sets a minimum 'age' for landing files
  //    to be deleted, avoiding data loss on compaction operation
  val landingPurgeTolerance = config.getDuration("landing_purge_tolerance")


  // pipeline settings
  val triggerTime = config.getDuration("trigger_time")
  val compactionRecordInterval = config.getLong("compaction_record_interval")

  // pipeline – Kafka settings
  val kafkaBootstrapServers = config.getString("kafka_bootstrap_servers")
  val kafkaTopic = config.getString("kafka_topic")


  // cache settings
  val cacheDataframes = config.hasPath("cache_dataframes") && config.getBoolean("cache_dataframes")
  val cacheExpiry = config.getDuration("cache_expiry")
  // controls after how much time should we evict the oldest cache, after finish a new computation
  val cleanPastCacheDelay = config.getDuration("clean_past_cache_delay")

  // graphite metrics settings
  val graphiteHost = config.getString("graphite.hostname")
  val graphitePort = config.getInt("graphite.port")


  import scala.collection.JavaConversions._
  override def toString: String = config.entrySet().map { entry =>
      s"  ${entry.getKey} = ${entry.getValue.toString}"
    }.mkString("\n")
}
