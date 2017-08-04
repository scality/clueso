package com.scality.clueso

import com.typesafe.config.Config

class CluesoConfig(config: Config) {
  val s3SslEnabled = config.getString("s3_ssl_enabled")
  val s3Endpoint = config.getString("s3_endpoint")
  val s3AccessKey = config.getString("s3_access_key")
  val s3SecretKey = config.getString("s3_secret_key")
  val checkpointPath = config.getString("checkpoint_path")
  val outputBucketName = config.getString("output_bucket_name")
  val outputBucketPath = config.getString("output_bucket_path")
  val outputPath = s"s3a://$outputBucketName$outputBucketPath"
  val mergePath = config.getString("merge_path")
  val mergeFrequency = config.getDuration("merge_frequency")
  val triggerTime = config.getDuration("trigger_time")
  val mergeFactor = config.getInt("merge_factor")
  val kafkaBootstrapServers = config.getString("kafka_bootstrap_servers")
  val kafkaTopic = config.getString("kafka_topic")
}
