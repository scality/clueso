package com.scality.clueso

import org.apache.spark.sql.types._

object CluesoConstants {

  val sstSchema = new StructType()
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
      .add("location", new ArrayType(StringType, false))
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
      .add("isDeleteMarker", BooleanType)
      .add("x-amz-version-id", StringType))
}
