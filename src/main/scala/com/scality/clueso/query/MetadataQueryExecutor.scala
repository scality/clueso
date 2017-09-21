package com.scality.clueso.query

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference
import java.util.{concurrent => juc}

import com.scality.clueso.{CluesoConfig, CluesoConstants, SparkUtils}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.clueso.metrics.SearchMetricsSource
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.joda.time.DateTime

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, TimeoutException}

class MetadataQueryExecutor(spark : SparkSession, config : CluesoConfig) extends LazyLogging {
  import MetadataQueryExecutor._

  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  SearchMetricsSource(spark, config)

  Future {
    while (true) {
      Thread.sleep(5000)

      logger.info("Registrying new metrics")
      SearchMetricsSource.registerRddMetrics(spark)
      logger.info("Done")
    }
  }

  def printResults(f : Future[Array[String]], duration: Duration) : Int = {
    try {
      val result = Await.result[Array[String]](f, duration)
      println(s"RESULT:[${result.mkString(",")}]")
      -1
    } catch {
      case t:TimeoutException => {
        println("")
        -1
      }
      case t:Throwable => {
        logger.error("Error while printing results", t)
        println(s"ERROR:${t.toString}")
        -1
      }
    }

  }

  printResults(kpn, scala.concurrent.duration.Duration.apply(200, java.util.concurrent.TimeUnit.MILLISECONDS))

  def execAsync(query : MetadataQuery) = {
    Future {
      SparkUtils.getQueryResults(spark, this, query)
    }
  }

  def executeAndPrint(query : MetadataQuery) = {
    val resultArray = SparkUtils.getQueryResults(spark, this, query)
    println("[" +  resultArray.mkString(",") + "]")
  }

  var bucketDfs = Map[String, AtomicReference[DataFrame]]()
  var bucketUpdateTs = Map[String, DateTime]()


  val setupDfLock = new scala.collection.parallel.mutable.ParHashSet[String]()

  def acquireLock(bucketName : String) = synchronized {
    if (!setupDfLock.contains(bucketName)) {
      setupDfLock += bucketName
      true
    } else
      false
  }

  def releaseLock(bucketName : String) = synchronized { setupDfLock -= bucketName }

  def getBucketDataframe(bucketName : String) : DataFrame = {
    if (!config.cacheDataframes) {
      setupDf(spark, config, bucketName)
    } else {
      // check if cache doesn't exist
      val tableView = s"${(Math.random()*100).toInt}_$bucketName"
      if (!bucketDfs.contains(bucketName)) {
        val bucketDf = setupDf(spark, config, bucketName)

        if (acquireLock(bucketName)) {
          bucketDf.createOrReplaceTempView(tableView)
          bucketDf.sparkSession.catalog.cacheTable(tableView)

          bucketDfs = bucketDfs.updated(bucketName, new AtomicReference(bucketDf))
          bucketUpdateTs = bucketUpdateTs.updated(bucketName, DateTime.now())
          releaseLock(bucketName)
        }

        bucketDf
      } else {
        // cache exists
        if (bucketUpdateTs(bucketName).plus(config.mergeFrequency.toMillis).isBeforeNow) {
          // check if there's a dataframe update going on
          if (acquireLock(bucketName)) {

            // async update dataframe if there's none already doing it
            Future {
              logger.info(s"Calculating view $tableView")
              val bucketDf = setupDf(spark, config, bucketName)

              bucketDf.createOrReplaceTempView(tableView)

              // this operation triggers execution
              val cachingAsync = Future {
                bucketDf.sparkSession.catalog.cacheTable(tableView)
              }
              import scala.concurrent.duration._
              Await.ready(cachingAsync, 3 minutes)

              bucketDf
            } map { bucketDf =>
              logger.info(s"Calculating view $tableView")
              bucketDf.count() // force calculation
              logger.info(s"Atomically swapping RDD for bucket = $bucketName ( new = $tableView )")
              val oldDf = bucketDfs.getOrElse(bucketName, new AtomicReference()).getAndSet(bucketDf)
              bucketUpdateTs = bucketUpdateTs.updated(bucketName, DateTime.now())
              releaseLock(bucketName) // unlock

              // sleep before deleting oldDf
              Thread.sleep(10000) // TODO make configurable
              logger.info(s"Unpersisting ${oldDf.rdd.name} after 10sec")
              oldDf.unpersist(true)
            }
          }

          // set the time as updated, to avoid triggering it again
          bucketUpdateTs = bucketUpdateTs.updated(bucketName, DateTime.now())
        }

        //  return most recent cached version (always)
        bucketDfs(bucketName).get()
      }
    }
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

    val currentWorkers = Math.max(currentActiveExecutors(spark.sparkContext).count(_ => true), 1)

    resultDf
      .select(CluesoConstants.resultCols: _*)
      .orderBy(col("key"))
      .limit(query.limit)
  }
}

object MetadataQueryExecutor extends LazyLogging {

  /** Method that just returns the current active/registered executors
    * excluding the driver.
    * @param sc The spark context to retrieve registered executors.
    * @return a list of executors each in the form of host:port.
    */
  def currentActiveExecutors(sc: SparkContext): Seq[String] = {
    val allExecutors = sc.getExecutorMemoryStatus.map(_._1)
    val driverHost: String = sc.getConf.get("spark.driver.host")
    allExecutors.filter(! _.split(":")(0).equals(driverHost)).toList
  }

  // columns in Parquet files (landing and staging)
  val cols = Array(col("bucket"),
    col("kafkaTimestamp"),
    col("key"),
    col("type"),
    col("message"))

  def getColdStagingTable(spark : SparkSession, config : CluesoConfig, bucketName : String) = {
    val _stagingTable = spark.read
      .schema(CluesoConstants.storedEventSchema)
      .parquet(config.stagingPath)
      .createOrReplaceTempView("staging")

    spark.catalog.refreshTable("staging")

    // read df
    val stagingTable = spark.table("staging")
      .where(col("bucket").eqNullSafe(bucketName))
      .select(cols: _*)
    //      .orderBy("key")

    stagingTable
  }

  def getColdLandingTable(spark : SparkSession, config : CluesoConfig, bucketName : String) = {
    val _landingTable = spark.read
      .schema(CluesoConstants.storedEventSchema)
      .parquet(config.landingPath)
      .createOrReplaceTempView("landing")


    var landingTable = spark.table("landing")
      .where(col("bucket").eqNullSafe(bucketName))
      .select(cols: _*)
    //      .orderBy("key")
    landingTable
  }

  // returns data frame together with wasCached boolean
  def setupDf(spark : SparkSession, config : CluesoConfig, bucketName : String) : Dataset[Row]= {
    val currentWorkers = currentActiveExecutors(spark.sparkContext).count(_ => true)

    logger.info(s"Calculating DFs for bucket $bucketName – current workers = $currentWorkers")

    // read df
    var stagingTable = getColdStagingTable(spark, config, bucketName)

    if (currentWorkers > 0) {
      stagingTable = stagingTable.repartition(currentWorkers)
    }

    var landingTable = getColdLandingTable(spark, config, bucketName)

    if (currentWorkers > 0) {
      landingTable = landingTable.repartition(currentWorkers)
    }

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


    var result = landingTable.select(fillNonExistingColumns(colsLanding, unionCols):_*)
      .union(stagingTable.select(fillNonExistingColumns(colsStaging, unionCols):_*))
      .orderBy(col("key"))
      .withColumn("rank", dense_rank().over(windowSpec))
      .where(col("rank").lt(2).and(col("type") =!= "delete"))
      .select(col("bucket"),
        col("kafkaTimestamp"),
        col("key"),
        col("type"),
        col("message.`userMd`").as("userMd"),
        col("message.`incrementer`").as("incrementer"),
        col("message.`sourceFileName`").as("sourceFileName"),
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

    var query : Option[MetadataQuery] = None

    query = Some(MetadataQuery(bucketName, sqlWhereExpr, start_key = None, limit = 1000))
    queryExecutor.executeAndPrint(query.get)

  }
}
