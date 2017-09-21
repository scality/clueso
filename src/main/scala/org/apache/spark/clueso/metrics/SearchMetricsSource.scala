package org.apache.spark.clueso.metrics

import java.util.concurrent.TimeUnit

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.scality.clueso.CluesoConfig
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.metrics.source.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.RDDInfo

import scala.collection.mutable

object SearchMetricsSource extends LazyLogging {
  def countSearches(i: Int) = {
    searchMetricsSource.map(_.METRIC_SESSION_SEARCHES.inc(i))
  }

  var searchMetricsSource : Option[SearchMetricsSource] = None

  def registerRddMetrics(sparkSession: SparkSession) = {
    searchMetricsSource.map(source => {
      source.registerRddMetrics(sparkSession)
    })

    searchMetricsSource.map(_.sendToGraphite)
  }

  def apply(sparkSession: SparkSession, config: CluesoConfig): SearchMetricsSource =
    new SearchMetricsSource(sparkSession, config)
}
class SearchMetricsSource(sparkSession: SparkSession, config : CluesoConfig) extends Source
  with LazyLogging {

  sys.addShutdownHook {
    logger.info("Shutting down Search Metrics Source")
    graphite.foreach(_.close())
  }

  val _metricRegistry: MetricRegistry = new MetricRegistry()

  val graphite = if (config.graphiteHost.isEmpty)
    None
  else
    Some(new Graphite(config.graphiteHost, config.graphitePort))

  val reporter = graphite.map(GraphiteReporter.forRegistry(metricRegistry)
      .prefixedWith(s"spark.clueso_searcher.driver.search")
      .build(_))

  reporter.map(_.start(5, TimeUnit.SECONDS))

  override def sourceName: String = "search"
  def metricRegistry: MetricRegistry = _metricRegistry

  SearchMetricsSource.searchMetricsSource = Some(this)

  def sendToGraphite() = {
    logger.info("Reporting metrics")
    reporter.map(_.report(
        metricRegistry.getGauges,
        metricRegistry.getCounters,
        metricRegistry.getHistograms,
        metricRegistry.getMeters,
        metricRegistry.getTimers
      )
    )
  }

  // bucketName -> published tableName
  val registered = mutable.HashMap[String,String]()

  /**
    * Tracks the total number of searches in the session
    */
  val METRIC_SESSION_SEARCHES = metricRegistry.counter(MetricRegistry.name("session_search_count"))

  // swipes all available rdd storage info and adds them to the list, if they don't exist already
  def registerRddMetrics(sparkSession: SparkSession): Unit = {
    val pattern = "In-memory table ([0-9]+)_([A-Za-z]+)".r


    sparkSession.sparkContext.getRDDStorageInfo
      .filter(_.isCached)
      .filter { rddInfo =>

      logger.debug(s"Filtering rddInfo Name = ${rddInfo.name}")
      pattern.findFirstMatchIn(rddInfo.name).isDefined
    } map { rddInfo =>

      val pattern(rndNumber, bucketName) = rddInfo.name

      logger.info(s"Transforming rddInfo Name = ${rddInfo.name} into $bucketName ")

      ((s"${rndNumber}_$bucketName", bucketName), rddInfo)
    } filter { case ((tableName, bucketName), rddInfo) =>
      val result = !registered.contains(bucketName) || registered.getOrElse(bucketName, "").equals(tableName)

      logger.debug(s"Checking presence of $bucketName in registered = ${registered.mkString(",")} ")

      result
    } foreach { case ((tableName, bucketName), rddInfo) =>

      logger.info(s"Metrics – registring for RDD = ${bucketName} ( $tableName )")

      registerRddMetric(sparkSession, bucketName, "numPartitions", _.numPartitions)
      registerRddMetric(sparkSession, bucketName, "numCachedPartitions", _.numCachedPartitions)
      registerRddMetric(sparkSession, bucketName, "memSize", _.memSize)
      registerRddMetric(sparkSession, bucketName, "diskSize", _.diskSize)
      registerRddMetric(sparkSession, bucketName, "externalBlockStoreSize", _.externalBlockStoreSize)
      registered.put(bucketName, tableName)
    }


  }

  def registerRddMetric(sparkSession : SparkSession, rddInfoName : String, measurementName : String, extractor: RDDInfo => Long): Unit = {
    val rddTag = rddInfoName.replaceAll("\\s+", "-")

    logger.info(s"Metrics – registring metric = $sourceName.$rddTag.$measurementName")
    metricRegistry.register(MetricRegistry.name(s"$rddTag.$measurementName"),
      new Gauge[Long] {
        override def getValue: Long = {
          if (!registered.contains(rddInfoName)) {
            0L
          } else {
            Option(sparkSession.sparkContext.getRDDStorageInfo
              .filter(_.name.equals(rddName(rddInfoName)))
            ).flatMap(rddInfos => {
              if (rddInfos.length > 0)
                Some(rddInfos.head)
              else
                None
            }).map(extractor) match {
              case Some(value) => value
              case None => 0L
            }
          }
        }
      })
  }

  def rddName(rddId : String) = {
    s"In-memory table `bucket=$rddId`"
  }
}
