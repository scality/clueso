package com.scality.clueso

import com.typesafe.config.Config

import scala.util.Properties

class CluesoConfig(config: Config) {
  val searchMetricsFlushFrequency: Long = {
    if (config.hasPath("search_metrics_flush_frequency")) {
      config.getDuration("search_metrics_flush_frequency").toMillis
    } else {
      5000
    }
  }

  def sparkSqlPrintExplain = envOrElseConfig("spark_sql_print_explain").toBoolean

  var sparkUiPortValue = 4050
  def setSparkUiPort(portNumber: Int) = {
    this.sparkUiPortValue = portNumber
  }

  def sparkUiPort = this.sparkUiPortValue

  // storage params
  def s3SslEnabled = envOrElseConfig("s3_ssl_enabled")
  def s3Endpoint = envOrElseConfig("s3_endpoint")
  def s3AccessKey = envOrElseConfig("aws_access_key_id")
  def s3SecretKey = envOrElseConfig("aws_secret_access_key")
  def s3PathStyleAccess = config.getString("s3_path_style_access")
  def checkpointPath = config.getString("checkpoint_path")

  // locations
  def bucketName = config.getString("bucket_name")
  def bucketLandingPath = config.getString("bucket_landing_path")
  def bucketStagingPath = config.getString("bucket_staging_path")

  def landingPathUri = s"s3a://$bucketName$bucketLandingPath"
  def stagingPathUri = s"s3a://$bucketName$bucketStagingPath"
  def checkpointUrl = s"s3a//$bucketName$checkpointPath"

  // compaction
  //    as S3 object store is eventual consistency, this sets a minimum 'age' for landing files
  //    to be deleted, avoiding data loss on compaction operation
  def landingPurgeTolerance = config.getDuration("landing_purge_tolerance")


  // pipeline settings
  def triggerTime = config.getDuration("trigger_time")
  def compactionRecordInterval = config.getLong("compaction_record_interval")

  // pipeline – Kafka settings
  def kafkaBootstrapServers = config.getString("kafka_bootstrap_servers")
  def kafkaTopic = config.getString("kafka_topic")


  // cache settings
  def cacheDataframes = config.hasPath("cache_dataframes") && config.getBoolean("cache_dataframes")
  def cacheExpiry = config.getDuration("cache_expiry")
  // controls after how much time should we evict the oldest cache, after finish a new computation
  def cleanPastCacheDelay = config.getDuration("clean_past_cache_delay")

  // graphite metrics settings
  def graphiteHost = envOrElseConfig("graphite.hostname")
  def graphitePort = envOrElseConfig("graphite.port").toInt


  def envOrElseConfig(name: String): String = {
    Properties.envOrElse(
      name.toUpperCase.replaceAll("""\.""", "_"),
      if (config.hasPath(name)) {
        config.getString(name)
      } else {
        ""
      }
    )
  }


  import scala.collection.JavaConversions._
  override def toString: String = config.entrySet().map { entry =>
      s"  ${entry.getKey} = ${entry.getValue.toString}"
    }.mkString("\n")
}
