package com.scality.clueso.query

import com.scality.clueso.{CluesoConfig, CluesoConstants, SparkUtils}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

class MetadataQuery(spark : SparkSession, config: CluesoConfig, bucketName : String, sqlWhereExpr : String, start : Int, end : Int) {
  override def toString: String = s"[MetadataQuery bucket=${bucketName}, query=$sqlWhereExpr, start=$start, end=$end]"

  def hasColumn(df: DataFrame, path: String) = Try(df(path)).isSuccess

  def execute() = {
    val cols = Array(col("bucket"),
      col("kafkaTimestamp"),
      col("key"),
      col("type"),
      col("message"))

    // read df
    val stagingTable = spark.read
      .schema(CluesoConstants.storedEventSchema)
      .parquet(config.stagingPath)
      .where(col("bucket").eqNullSafe(bucketName))
      .select(cols: _*)
      .orderBy("key")

    val landingTable = spark.read
      .schema(CluesoConstants.storedEventSchema)
      .parquet(config.landingPath)
      .where(col("bucket").eqNullSafe(bucketName))
      .select(cols: _*)
      .orderBy("key")

//    // debug code
//    println("staging schema:")
//    stagingTable.printSchema()
//
//    println("landing schema:")
//    landingTable.printSchema()
//    var result = stagingTable.collect()
//    result = landingTable.collect()



    val colsLanding = landingTable.columns.toSet
//    val colsLanding = landingTable.schema.fields.map(_.name).toSet
    val colsStaging = stagingTable.columns.toSet
    val unionCols = colsLanding ++ colsStaging

    // window function over union of partitions bucketName=<specified bucketName>
    val windowSpec = Window.partitionBy("key").orderBy(col("kafkaTimestamp").desc)

    import SparkUtils.fillNonExistingColumns
    var union = landingTable.select(fillNonExistingColumns(colsLanding, unionCols):_*)
      .union(stagingTable.select(fillNonExistingColumns(colsStaging, unionCols):_*))
      .orderBy(col("key"))
      .withColumn("rank", dense_rank().over(windowSpec))
      .where(col("rank").lt(2).and(col("type") =!= "delete"))


    if (!sqlWhereExpr.trim.isEmpty) {
      union = union.where(sqlWhereExpr)
    }


    union.select(CluesoConstants.resultCols: _*)
  }
}

object MetadataQueryExecutor {

  def main(args: Array[String]): Unit = {
    require(args.length > 2, "Usage: ./command /path/to/application.conf bucket sqlWhereQuery")

    val config = SparkUtils.loadCluesoConfig(args(0))

    val spark = SparkUtils.buildSparkSession(config)
      .appName("Query Executor")
      .getOrCreate()

//    val queryExec = new MetadataQueryExecutor(spark, config)

    val bucketName = args(1) // e.g.   "wednesday"
    val sqlWhereExpr = args(2) // "color=blue AND (y=x OR y=g)"
    val query = new MetadataQuery(spark, config, bucketName, sqlWhereExpr, start = 0, end = 1000)

    query.execute()
//    val filter = Filter("bucket", "gobig")
//    val result = query.execute(filter)
//    println(result)
  }

}
