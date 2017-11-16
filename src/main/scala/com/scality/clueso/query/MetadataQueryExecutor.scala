package com.scality.clueso.query

import java.util.concurrent.atomic.AtomicBoolean
import java.util.{concurrent => juc}

import com.scality.clueso._
import com.scality.clueso.query.cache.SessionCacheManager
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.clueso.metrics.SearchMetricsSource
//import org.apache.spark.clueso.metrics.SearchMetricsSource
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.concurrent.{ExecutionContext, Future}

class MetadataQueryExecutor(spark : SparkSession, config : CluesoConfig) extends LazyLogging {

  import MetadataQueryExecutor._

  import ExecutionContext.Implicits.global

    SearchMetricsSource(spark, config)

  val metricsRegisterCancel = new AtomicBoolean(false)

  val metricsRegisterThread = Future {
    while (!metricsRegisterCancel.get()) {
      Thread.sleep(config.searchMetricsFlushFrequency)

      logger.debug("Registering new metrics")
      SearchMetricsSource.registerRddMetrics(spark)
      logger.debug("Done")
    }
  }

  sys.addShutdownHook {
    metricsRegisterCancel.set(true)
  }


  def getBucketDataframe(bucketName : String) : DataFrame = {
    if (!config.cacheDataframes) {
      logger.info("Cache is off.")
      setupDf(spark, config, bucketName)
    } else {
      // delegate to cache manager
      SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
    }
  }

  /**
    * This returns the result to Livy via STDOUT. The println will output a valid JSON string
    * with results that are then parsed by S3 before returning to client
    *
    */
  def executeAndPrint(query : MetadataQuery) = {
    val resultArray = SparkUtils.getQueryResults(spark, this, query)
    println("[" +  resultArray.mkString(",") + "]")
  }

  def execute(query : MetadataQuery) = {
    val bucketName = query.bucketName

    SearchMetricsSource.countSearches(1)

    var resultDf = getBucketDataframe(bucketName)

    val sqlWhereExpr = query.sqlWhereExpr
    if (!sqlWhereExpr.isEmpty) {
      resultDf = resultDf.where(sqlWhereExpr)
    }

    if (query.start_key.isDefined) {
      resultDf = resultDf.where(col("key") > lit(query.start_key.get))
    }

    val currentWorkers = Math.max(currentActiveExecutors(spark.sparkContext), 1)

    val result = resultDf
      .select(CluesoConstants.resultCols: _*)
      .orderBy(col("key"))
      .limit(query.limit)

    if (config.sparkSqlPrintExplain) {
      logger.info("Explain Query:")
      result.explain(true)
      logger.info("End Explain Query")
    }

    result
  }
}

object MetadataQueryExecutor extends LazyLogging {

  /** Method that just returns the count of current active/registered executors
    * excluding the driver.
    * @param sc The spark context to retrieve registered executors.
    * @return count of executors
    */
  def currentActiveExecutors(sc: SparkContext): Int = {

    val allExecutors = sc.getExecutorMemoryStatus.keys // keys contain workers hostnames (format = host:port)
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).count(_ => true)
  }

  // columns in Parquet files (landing and staging)
  val cols = Array(col("bucket"),
    col("opIndex"),
    col("key"),
    col("type"),
    col("message"))

  def getColdStagingTable(spark : SparkSession, config : CluesoConfig, bucketName : String) = {
    logger.info(s"Reading cold staging table ${PathUtils.stagingURI(config)}")

    val _stagingTable = spark.read
      .schema(CluesoConstants.storedEventSchema)
      .parquet(PathUtils.stagingURI(config))
      .createOrReplaceTempView("staging")

    // what does this do?
    // why not in Landing?
    spark.catalog.refreshTable("staging")

    val stagingTable = spark.table("staging")
      .where(col("bucket").eqNullSafe(bucketName))
      .select(cols: _*)

    stagingTable
  }

  def getColdLandingTable(spark : SparkSession, config : CluesoConfig, bucketName : String) = {
    logger.info(s"Reading cold landing table ${PathUtils.landingURI(config)}")

    val _landingTable = spark.read
      .schema(CluesoConstants.storedEventSchema)
      .parquet(PathUtils.landingURI(config))
      .createOrReplaceTempView("landing")

    var landingTable = spark.table("landing")
      .where(col("bucket").eqNullSafe(bucketName))
      .select(cols: _*)

    landingTable
  }

  // returns data frame
  def setupDf(spark : SparkSession, config : CluesoConfig, bucketName : String) : Dataset[Row]= {
    val currentWorkers = Math.max(currentActiveExecutors(spark.sparkContext), 1)

    logger.info(s"Calculating DFs for bucket $bucketName – current workers = $currentWorkers")

    // read df
    var stagingTable = getColdStagingTable(spark, config, bucketName)
    var landingTable = getColdLandingTable(spark, config, bucketName)

    val colsLanding = landingTable.columns.toSet
    val colsStaging = stagingTable.columns.toSet
    val unionCols = colsLanding ++ colsStaging

    // window function over union of partitions bucketName=<specified bucketName>
    val windowSpec = Window.partitionBy("key").orderBy(col("opIndex").desc)

    import SparkUtils.fillNonExistingColumns

    var result = landingTable.select(fillNonExistingColumns(colsLanding, unionCols):_*)
      .union(stagingTable.select(fillNonExistingColumns(colsStaging, unionCols):_*))
      .withColumn("rank", row_number().over(windowSpec))
      .where(col("rank").lt(2).and(col("type") =!= "delete"))
      .select(col("bucket"),
        col("opIndex"),
        col("key"),
        col("type"),
        col("message.`userMd`").as("userMd"),
        col("message.`dataStoreName`").as("dataStoreName"),
        col("message.`acl`").as("acl"),
        col("message.`location`").as("location"),
        col("message.`tags`").as("tags"),
        col("message.`replicationInfo`").as("replicationInfo"),
        col("message.`md-model-version`").as("md-model-version"),
        col("message.`owner-display-name`").as("owner-display-name"),
        col("message.`owner-id`").as("owner-id"),
        col("message.`content-length`").as("content-length"),
        col("message.`content-type`").as("content-type"),
        col("message.`last-modified`").as("last-modified"),
        col("message.`content-md5`").as("content-md5"),
        col("message.`x-amz-server-version-id`").as("x-amz-server-version-id"),
        col("message.`x-amz-storage-class`").as("x-amz-storage-class"),
        col("message.`x-amz-server-side-encryption`").as("x-amz-server-side-encryption"),
        col("message.`x-amz-server-side-encryption-aws-kms-key-id`").as("x-amz-server-side-encryption-aws-kms-key-id"),
        col("message.`x-amz-server-side-encryption-customer-algorithm`").as("x-amz-server-side-encryption-customer-algorithm"),
        col("message.`x-amz-website-redirect-location`").as("x-amz-website-redirect-location"),
        col("message.`isDeleteMarker`").as("isDeleteMarker"),
        col("message.`x-amz-version-id`").as("x-amz-version-id"))

    if (currentWorkers > 0) {
      result = result.coalesce(currentWorkers)
    }

    result
  }

  def apply(spark: SparkSession, config: CluesoConfig): MetadataQueryExecutor = {
    new MetadataQueryExecutor(spark, config)
  }

  def main(args: Array[String]): Unit = {
    require(args.length > 2, "Usage: ./command /path/to/application.conf bucket sqlWhereQuery")

    val config = SparkUtils.loadCluesoConfig(args(0))

    val spark = SparkUtils.buildSparkSession(config)
      .appName("Query Executor")
      .getOrCreate()

    val bucketName = args(1) // e.g.   "wednesday"
    val sqlWhereExpr = args(2) // "color=blue AND (y=x OR y=g)"


    val queryExecutor = MetadataQueryExecutor(spark, config)

    val query = MetadataQuery(bucketName, sqlWhereExpr, start_key = None)
    queryExecutor.executeAndPrint(query)

  }
}
