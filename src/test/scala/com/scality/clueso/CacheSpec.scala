package com.scality.clueso

import com.scality.clueso.query.cache.SessionCacheManager
import com.scality.clueso.query.cache.SessionCacheManager.logger
import com.scality.clueso.tools.LandingMetadataPopulatorTool
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class CacheSpec extends WordSpec with Matchers with SparkContextSetup {
  "SessionCacheManager" should {
    "Scenario 1: cache dataframe and return dataframe" in withSparkContext {
      (spark, config) =>
        val bucketName = "cachebucket"
        val fs = SparkUtils.buildHadoopFs(config)
        implicit val _config = config
        fs.mkdirs(new Path(PathUtils.stagingURI))
        fs.mkdirs(new Path(PathUtils.landingURI))
        fs.mkdirs(new Path(PathUtils.landingURI concat("/bucket=") concat(bucketName)))
        val df = SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
        SessionCacheManager.bucketDfs(bucketName).get() should be theSameInstanceAs df
        df.count() shouldBe 0

        // populate landing
        val numberParquetFiles = "10"
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "10", numberParquetFiles))

        // wait to update cache
        val wait = config.cacheExpiry.toMillis + 1000
        logger.info(s"Sleeping so cache expires for $wait ms")
        Thread.sleep(wait)

        val updatedDf = SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
        updatedDf.count shouldEqual 10
        SessionCacheManager.bucketDfs(bucketName).get().count() shouldEqual 10
        SessionCacheManager.bucketDfs(bucketName).get() should be theSameInstanceAs updatedDf

    }
  }
}
