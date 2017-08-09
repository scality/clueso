package com.scality.clueso

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

}
