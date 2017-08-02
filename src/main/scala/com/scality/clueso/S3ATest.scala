package com.scality.clueso

import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object S3ATest {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
//      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
//      .config("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:8000")
//      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//      .config("spark.hadoop.fs.s3a.access.key", "accessKey1")
//      .config("spark.hadoop.fs.s3a.secret.key", "verySecretKey1")
      .master("local[*]")
      .appName("S3ATest")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

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


    val eventStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test2")
      // TODO custom offsets depending on recovery point
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load()


    val writeStream = eventStream.select(
        col("key").cast("string"),
        col("partition").cast("integer"),
        col("offset").cast("integer"),
        col("topic").cast("string"),
        from_json(col("value").cast("string"), sstSchema).alias("message")
      )
      .filter(row => row.getStruct(row.fieldIndex("message")) != null)
      .select(col("key"), col("message.bucket").alias("bucket"), col("message"))
      .writeStream
      .trigger(ProcessingTime("5 seconds"))

    val query = writeStream
      .option("checkpointLocation", "./_checkpoint")
      .format("parquet")
      .partitionBy("bucket")
      .option("path", "s3a://mybucket/output")
      .start()

    query.awaitTermination()
  }
}
