package com.scality.clueso.metrics

import com.scality.clueso.{SparkContextSetup}
import org.apache.spark.clueso.metrics.SearchMetricsSource
import org.scalatest.{Matchers, WordSpec}

class SearchMetricsSpec extends WordSpec with Matchers with SparkContextSetup {
  "SearchMetricsSource" should {
    "Scenario 1: parseRddInfoName parses rdd name" in withSparkContext {
      (spark, config) =>
      val SearchSource: SearchMetricsSource = new SearchMetricsSource(spark, config)
      val rddInfoName = "In-memory table 1515439088487_basicsearchmebucket1515439031713"
      val (rndNumber, bucketName) = SearchSource.parseRddInfoName(rddInfoName)
      rndNumber shouldEqual("1515439088487")
      bucketName shouldEqual("basicsearchmebucket1515439031713")
    }

    "Scenario 2: parseRddInfo returns empty string for invalid bucketName" in withSparkContext {
      (spark, config) =>
        val SearchSource: SearchMetricsSource = new SearchMetricsSource(spark, config)
        val rddInfoName = "In-memory table 1515439088487_INVALID*BUCKET31713"
        val (rndNumber, bucketName) = SearchSource.parseRddInfoName(rddInfoName)
        rndNumber shouldEqual("")
        bucketName shouldEqual("")
    }
  }
}
