package com.scality.clueso

import com.scality.clueso.compact.TableFilesCompactor
import com.scality.clueso.query.cache.SessionCacheManager
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

        // populate landing
        val numberParquetFiles = "10"
        val originalEntries = "15"
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, originalEntries, numberParquetFiles))

        val df = SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
        SessionCacheManager.bucketDfs(bucketName).get() should be theSameInstanceAs df
        df.count() shouldBe originalEntries.toInt

        // populate landing again
        val updatedEntries = "10"
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, updatedEntries, numberParquetFiles))

        // checking that retrieve stale data from cache since cache has not expired
        val staleDf = SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
        SessionCacheManager.bucketDfs(bucketName).get() should be theSameInstanceAs df
        staleDf.count() shouldBe originalEntries.toInt

        // wait for cache to update
        val wait = config.landingCacheExpiry + 1000
        logger.info(s"Sleeping so cache expires: $wait ms")
        Thread.sleep(wait)

        // updated cache should only have new entries
        val updatedDf = SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
        updatedDf.count shouldEqual (updatedEntries.toInt)
        SessionCacheManager.bucketDfs(bucketName).get().count() shouldEqual (updatedEntries.toInt)
        SessionCacheManager.bucketDfs(bucketName).get() should be theSameInstanceAs updatedDf

        // wait for cache to expire again
        logger.info(s"Sleeping so cache expires: $wait ms")
        Thread.sleep(wait)

        // repopulate landing again
        val newNumberParquetFiles = "11"
        val newEntries = "11"
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, newEntries, newNumberParquetFiles))

        // should be able to calculate cache again even though an old cache has been deleted
        val retryDf = SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
        SessionCacheManager.bucketDfs(bucketName).get().count() shouldEqual (newEntries.toInt)
        SessionCacheManager.bucketDfs(bucketName).get() should be theSameInstanceAs retryDf

    }
    "Scenario 2: cache properly after compaction" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactioncachebucket"
        val fs = SparkUtils.buildHadoopFs(config)
        implicit val _config = config
        fs.mkdirs(new Path(PathUtils.stagingURI))
        fs.mkdirs(new Path(PathUtils.landingURI))
        fs.mkdirs(new Path(PathUtils.landingURI concat("/bucket=") concat(bucketName)))

        // populate landing
        val numberParquetFiles = "10"
        val originalEntries = "15"
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, originalEntries, numberParquetFiles))

        val df = SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
        SessionCacheManager.bucketDfs(bucketName).get() should be theSameInstanceAs df
        df.count() shouldBe originalEntries.toInt

        // compact files from landing to staging (calling this causes process to wait for landingPurgeTolerance time)
        // so both landing and staging cache should have expired by then
        val compactor = new TableFilesCompactor(spark, config)
        compactor.compactLandingPartition("bucket", bucketName, 1, true)

        // checking that retrieve same data in different format from cache since cache expired
        val updatedDf = SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
        SessionCacheManager.bucketDfs(bucketName).get() should be theSameInstanceAs updatedDf
        updatedDf.count() shouldBe originalEntries.toInt

//        // wait for landing cache to update
//        val wait = config.landingCacheExpiry + 1000
//        logger.info(s"Sleeping so cache expires: $wait ms")
//        Thread.sleep(wait)

//        // updated cache should have no entries since we artificially
//        val updatedDf = SessionCacheManager.getCachedBucketDataframe(spark, bucketName)(config)
//        updatedDf.count shouldEqual (updatedEntries.toInt)
//        SessionCacheManager.bucketDfs(bucketName).get().count() shouldEqual (updatedEntries.toInt)
//        SessionCacheManager.bucketDfs(bucketName).get() should be theSameInstanceAs updatedDf
//

    }
  }
}
