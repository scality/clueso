package com.scality.clueso

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object CluesoConstants {
  // order does matter:

  val replicationInfoSchema = new StructType()
    .add("status", StringType)
    .add("content", new ArrayType(StringType, false))
    .add("destination", StringType)
    .add("storageClass", StringType)
    .add("role", StringType)

  val locationSchema = new StructType()
    .add("key", StringType, false)
    .add("size", LongType, false)
    .add("start", LongType, false)
    .add("dataStoreName", StringType)
    .add("dataStoreETag", StringType)

  val eventAclSchema = new StructType()
    .add("Canned", StringType)
    .add("FULL_CONTROL", new ArrayType(StringType, false))
    .add("WRITE_ACP", new ArrayType(StringType, false))
    .add("READ", new ArrayType(StringType, false))
    .add("READ_ACP", new ArrayType(StringType, false));

  val eventValueSchema = new StructType()
    .add("userMd", MapType(StringType, StringType))
    .add("bucket", StringType)
    .add("key", StringType)
    .add("acl", new StructType(eventAclSchema.fields), false)
    .add("location", new ArrayType(locationSchema, false))
    .add("tags", new MapType(StringType, StringType, false))
    .add("replicationInfo", replicationInfoSchema)
    .add("md-model-version", IntegerType)
    .add("owner-display-name", StringType)
    .add("owner-id", StringType)
    .add("content-length", IntegerType)
    .add("content-type", StringType)
    .add("last-modified", StringType)
    .add("content-md5", StringType)
    .add("x-amz-server-version-id", StringType)
    .add("dataStoreName", StringType)
    .add("x-amz-storage-class", StringType)
    .add("x-amz-server-side-encryption", StringType)
    .add("x-amz-server-side-encryption-aws-kms-key-id", StringType)
    .add("x-amz-server-side-encryption-customer-algorithm", StringType)
    .add("x-amz-website-redirect-location", StringType)
    .add("isDeleteMarker", BooleanType)
    .add("x-amz-version-id", StringType)


  val eventSchema = new StructType()
    .add("opIndex", StringType)
    .add("type", StringType, false)
    .add("bucket", StringType)
    .add("key", StringType, false)
    .add("value", new StructType(eventValueSchema.fields), false)

  val storedEventSchema = new StructType()
    .add("opIndex", StringType)
    .add("bucket", StringType)
    .add("key", StringType, false)
    .add("kafkaTimestamp", TimestampType, false)
    .add("type", StringType, false)
    .add("message", new StructType(eventValueSchema.fields), false)

// TODO try to remove the ALIAS
  val resultCols = Seq(col("key"),
    col("`last-modified`"), //.as("`last-modified`"),
    col("`content-md5`"), //.as("`content-md5`"),
    col("`owner-id`"), //.as("`owner-id`"),
    col("`owner-display-name`"), //.as("`owner-display-name`"),
    col("`content-length`"), //.as("`content-length`"),
    col("`x-amz-storage-class`"), //.as("`x-amz-storage-class`"),
    col("bucket")
  )

  val resultSchema = new StructType()
    .add("key", StringType)
    .add("`last-modified`", StringType)
    .add("`content-md5`", StringType)
    .add("`owner-id`", StringType)
    .add("`owner-display-name`", StringType)
    .add("`content-length`", LongType)
    .add("`x-amz-storage-class`", StringType)
    .add("bucket", StringType)

}
