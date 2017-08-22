package com.scality.clueso.query

import java.util.concurrent.atomic.AtomicReference

import com.scality.clueso.{CluesoConfig, CluesoConstants, SparkUtils}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime


class MetadataQueryExecutor(spark : SparkSession, config : CluesoConfig) extends LazyLogging {
  import MetadataQueryExecutor._

  var bucketDfs = Map[String, AtomicReference[DataFrame]]()
  var bucketUpdateTs = Map[String, DateTime]()

  def execute(query : MetadataQuery) = {
    val bucketName = query.bucketName

    if (!bucketDfs.contains(bucketName)) {
      val bucketDf = setupDf(spark, config, bucketName)
      bucketDfs = bucketDfs.updated(bucketName, new AtomicReference(bucketDf))
      bucketUpdateTs = bucketUpdateTs.updated(bucketName, DateTime.now())
    } else {
      // check if the dataframe is 'old' and update
      if (bucketUpdateTs(bucketName).plus(config.mergeFrequency.toMillis).isBeforeNow) {
        // update dataframe
        val bucketDf = setupDf(spark, config, bucketName)

        bucketDfs(bucketName).get().unpersist(true)
        bucketDfs(bucketName).set(bucketDf)
      }
    }

    var resultDf = bucketDfs(bucketName).get()

    val sqlWhereExpr = query.sqlWhereExpr
    if (!sqlWhereExpr.isEmpty) {
      resultDf = resultDf.where(sqlWhereExpr)
    }


    resultDf.select(CluesoConstants.resultCols: _*)
  }
}

object MetadataQueryExecutor {

  def setupDf(spark : SparkSession, config : CluesoConfig, bucketName : String) = {
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


    val result = landingTable.select(fillNonExistingColumns(colsLanding, unionCols):_*)
      .union(stagingTable.select(fillNonExistingColumns(colsStaging, unionCols):_*))
      .orderBy(col("key"))
      .withColumn("rank", dense_rank().over(windowSpec))
      .where(col("rank").lt(2).and(col("type") =!= "delete"))

    result.cache()
  }

  def apply(spark: SparkSession, config: CluesoConfig): MetadataQueryExecutor = new MetadataQueryExecutor(spark, config)

  def main(args: Array[String]): Unit = {
    require(args.length > 2, "Usage: ./command /path/to/application.conf bucket sqlWhereQuery")

    val config = SparkUtils.loadCluesoConfig(args(0))

    val spark = SparkUtils.buildSparkSession(config)
      .appName("Query Executor")
      .getOrCreate()

//    val queryExec = new MetadataQueryExecutor(spark, config)

    val bucketName = args(1) // e.g.   "wednesday"
    val sqlWhereExpr = args(2) // "color=blue AND (y=x OR y=g)"


    val queryExecutor = MetadataQueryExecutor(spark, config)

    val query = MetadataQuery(spark, config, bucketName, sqlWhereExpr, start = 0, end = 1000)
    val result = queryExecutor.execute(query)

    println(result)
  }

}
