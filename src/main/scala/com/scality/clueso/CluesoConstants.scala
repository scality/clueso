package com.scality.clueso

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object CluesoConstants {
  // order does matter:

  val replicationInfoSchema = new StructType()
    .add("status", StringType, true)
    .add("content", new ArrayType(StringType, false), true)
    .add("destination", StringType, true)
    .add("storageClass", StringType, true)
    .add("role", StringType, true)

  val locationSchema = new StructType()
    .add("key", StringType, false)
    .add("size", LongType, false)
    .add("start", LongType, false)
    .add("dataStoreName", StringType, true)
    .add("dataStoreETag", StringType, true)

  val eventValueSchema = new StructType()
    .add("userMd", MapType(StringType, StringType, true), true)
    .add("bucket", StringType)
    .add("incrementer", StringType)
    .add("sourceFileName", StringType)
    .add("key", StringType)
    .add("value", new StructType()
      .add("acl", new StructType()
        .add("Canned", StringType)
        .add("FULL_CONTROL", new ArrayType(StringType, false))
        .add("WRITE_ACP", new ArrayType(StringType, false))
        .add("READ", new ArrayType(StringType, false))
        .add("READ_ACP", new ArrayType(StringType, false)))
      .add("location", new ArrayType(locationSchema, false))
      .add("tagging", new MapType(StringType, StringType, false), true)
      .add("replicationInfo", replicationInfoSchema)
      .add("md-model-version", IntegerType)
      .add("owner-display-name", StringType)
      .add("owner-id", StringType)
      .add("content-length", IntegerType)
      .add("content-type", StringType)
      .add("last-modified", StringType)
      .add("content-md5", StringType)
      .add("x-amz-server-version-id", StringType)
      .add("x-amz-storage-class", StringType)
      .add("x-amz-server-side-encryption", StringType)
      .add("x-amz-server-side-encryption-aws-kms-key-id", StringType)
      .add("x-amz-server-side-encryption-customer-algorithm", StringType)
      .add("x-amz-website-redirect-location", StringType)
      .add("isDeleteMarker", BooleanType)
      .add("x-amz-version-id", StringType))

  val eventSchema = new StructType()
    .add("type", StringType, true)
    .add("bucket", StringType)
    .add("incrementer", StringType, true)
    .add("sourceFileName", StringType, true)
    .add("key", StringType, false)
    .add("value", eventValueSchema, false)

  val storedEventSchema = new StructType()
    .add("kafkaTimestamp", TimestampType, false)
    .add("message", new StructType(eventSchema.fields), false)
    .add("bucket", StringType, false)


  val resultCols = Seq(col("message.key").as("key"),
    col("message.`value`.`last-modified`").as("`last-modified`"),
    col("message.`value`.`content-md5`").as("`content-md5`"), // etag
    col("message.`value`.`owner-id`").as("`owner-id`"),
    col("message.`value`.`owner-display-name`").as("`owner-display-name`"),
    col("message.`value`.`content-length`").as("`content-length`"),
    col("message.`value`.`x-amz-storage-class`").as("`x-amz-storage-class`"),
    col("bucket")
  )

  val resultSchema = new StructType()
    .add("key", StringType, true)
    .add("`last-modified`", StringType, true)
    .add("`content-md5`", StringType, true)
    .add("`owner-id`", StringType, true)
    .add("`owner-display-name`", StringType, true)
    .add("`content-length`", LongType, true)
    .add("`x-amz-storage-class`", StringType, true)
    .add("bucket", StringType, true)

}
