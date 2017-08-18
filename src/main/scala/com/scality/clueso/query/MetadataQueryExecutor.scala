package com.scality.clueso.query

import com.scality.clueso.{CluesoConfig, CluesoConstants, SparkUtils}
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
//    println("staging schema:")
//    stagingTable.printSchema()
//
//    println("landing schema:")
//    landingTable.printSchema()
//    var result = stagingTable.collect()
//    result = landingTable.collect()

    val colsLanding = landingTable.columns.toSet
    val colsStaging = stagingTable.columns.toSet
    val unionCols = colsLanding ++ colsStaging

    def expr(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(x => x match {
        case x if myCols.contains(x) => col(x)
        case _ => lit(null).as(x)
      })
    }

    // window function over union of partitions bucketName=<specified bucketName>
    val windowSpec = Window.partitionBy("message.key").orderBy(col("kafkaTimestamp").desc)

    var union = landingTable.select(expr(colsLanding, unionCols):_*)
      .union(stagingTable.select(expr(colsStaging, unionCols):_*))
      .orderBy(col("message.key"))
      .withColumn("rank", dense_rank().over(windowSpec))
      .where(col("rank").lt(2).and(col("message.type") =!= "delete"))


//    union.explain(true)

    if (!sqlWhereExpr.trim.isEmpty) {
      union = union.where(sqlWhereExpr)
    }



    union.select(CluesoConstants.resultCols: _*)
//    union
  }
}

object MetadataQueryExecutor {

  def main(args: Array[String]): Unit = {
    require(args.length > 2, "Usage: ./command /path/to/application.conf bucket sqlWhereQuery")

    val config = SparkUtils.loadCluesoConfig(args(0))

    val spark = SparkUtils.buildSparkSession(config)
//      .master("local[*]")
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
