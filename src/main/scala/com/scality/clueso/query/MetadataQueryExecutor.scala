package com.scality.clueso.query

import java.io.File

import com.scality.clueso.{CluesoConfig, CluesoConstants}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class MetadataQuery(spark : SparkSession, config: CluesoConfig, bucketName : String, sqlWhereExpr : String, start : Int, end : Int) {
  def execute() = {
    val cols = Array(col("bucket"),
      col("kafkaTimestamp"),
      col("message"))

    // read df
    val stagingTable = spark.read
      .schema(CluesoConstants.storedEventSchema)
      .parquet(config.stagingPath)
      .where(col("bucket").eqNullSafe(bucketName))
      .select(cols: _*)
      .orderBy("message.key")

    val landingTable = spark.read
      .schema(CluesoConstants.storedEventSchema)
      .parquet(config.landingPath)
      .where(col("bucket").eqNullSafe(bucketName))
      .select(cols: _*)
      .orderBy("message.key")

//    // debug code
    println("staging schema:")
    stagingTable.printSchema()

    println("landing schema:")
    landingTable.printSchema()

    var result = stagingTable.collect()
    result = landingTable.collect()

    // window function over union of partitions bucketName=<specified bucketName>
    val windowSpec = Window.partitionBy("message.key").orderBy(col("kafkaTimestamp").desc)

    var union = landingTable
      .union(stagingTable)
      .orderBy(col("message.key"))
      .withColumn("rank", dense_rank().over(windowSpec))
      .where(col("rank").lt(2).and(col("message.type") =!= "delete"))


    union.explain(true)

    if (!sqlWhereExpr.trim.isEmpty) {
      union = union.where(sqlWhereExpr)
    }


//    union.limit(end)
    union.select(col("message.*"))
  }
}

object MetadataQueryExecutor {

  def main(args: Array[String]): Unit = {
    require(args.length > 0, "specify configuration file")

    val parsedConfig = ConfigFactory.parseFile(new File(args(0)))
    val _config = ConfigFactory.load(parsedConfig)

    val config = new CluesoConfig(_config)

    val spark = SparkSession
      .builder
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", config.s3SslEnabled)
      .config("spark.hadoop.fs.s3a.endpoint", config.s3Endpoint)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.access.key", config.s3AccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", config.s3SecretKey)
      .master("local[*]")
      .appName("Query Executor")
      .getOrCreate()

//    val queryExec = new MetadataQueryExecutor(spark, config)

    val bucketName = "wednesday"
    val sqlWhereExpr = "" // "color=blue AND (y=x OR y=g)"
    val query = new MetadataQuery(spark, config, bucketName, sqlWhereExpr, start = 0, end = 1000)

    query.execute()
//    val filter = Filter("bucket", "gobig")
//    val result = query.execute(filter)
//    println(result)
  }

}
