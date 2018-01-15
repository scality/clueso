package com.scality.clueso.tools

import com.scality.clueso.compact.TableFilesCompactor
import com.scality.clueso.query.{MetadataQuery, MetadataQueryExecutor}
import com.scality.clueso.{SparkContextSetup, SparkUtils}
import org.apache.hadoop.fs.Path
import org.scalatest.{Matchers, WordSpec}

class CompactionSpec extends WordSpec with Matchers with SparkContextSetup {
  "Table Compactor" should {
    "Scenario 1: compact staging files with force" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactmebucket"
        val numberParquetFiles = "5"
        val compactionInterval = config.compactionRecordInterval
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)

        // populate
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "100", numberParquetFiles))

        // check landing files are created
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        // compact
        compactor.compactLandingPartition("bucket", bucketName, 1, true)

        // then, landing should be empty
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // then staging should have new object
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
    }

    "Scenario 2: compact staging files without specifying bucket name" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactnoname"
        val numberParquetFiles = "10"
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)

        // populate
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "100", numberParquetFiles))

        // check landing files were created correctly
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        //compact
        compactor.compact(1, true)

        // landing should be empty
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // should show staging obejct
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName/maxOpIndex=$numberParquetFiles"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
    }

    "Scenario 3: compact staging files in multiple buckets without specifying bucket name" in withSparkContext {
      (spark, config) =>
        val bucketName1 = "compactnoname1"
        val bucketName2 = "compactnoname2"
        val numberParquetFiles = "5"
        val compactionInterval = config.compactionRecordInterval
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)

        // populate both buckets
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName1, "100", numberParquetFiles))
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName2, "100", numberParquetFiles))

        // check landing files were created correctly
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName1/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName2/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        // compact
        compactor.compact(1, true)

        // landing should  be empty
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName1"), SparkUtils.parquetFilesFilter).length shouldEqual 0
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName2"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // check staging files were created for both
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName1/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName2/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
    }

    "Scenario 4: compact staging files without having to force compaction" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactnoforce"
        val numberParquetFiles = "5"
        val compactionInterval = config.compactionRecordInterval
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)

        // populate
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "1000", numberParquetFiles))

        // check that landing files were created
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        // compact
        compactor.compactLandingPartition("bucket", bucketName, 1, false)

        // check landing (should be empty)
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // should now have staging object
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual 1
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).foreach(item => println(item.getPath))
    }

    "Scenario 5: successful queries before and after compaction" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactquerycheck"
        val numberParquetFiles = "5"
        val compactionInterval = config.compactionRecordInterval
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)
        val queryExecutor = MetadataQueryExecutor(spark, config)
        val query = MetadataQuery(bucketName, "", None, 1000)

        // populate
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "1000", numberParquetFiles))

        // premake staging file
        // In deployment, the ingestion pipeline creates this
        fs.mkdirs(new Path(config.stagingPathUri))

        // check that landing files were created
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        // query should be successful before compaction, and all populated items should exist
        val resultBefore = queryExecutor.execute(query)
        resultBefore.count() shouldBe 1000

        // compact
        compactor.compactLandingPartition("bucket", bucketName, 1, false)

        // check landing (should be empty)
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // should now have a staging object
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldBe 1

        // query again should be successful after compaction, and all populated items should exist
        val resultAfter = queryExecutor.execute(query)
        resultAfter.count() shouldBe 1000
    }

    "Scenario 6: successful query with specific metadata criteria before and after compaction" in withSparkContext {
      (spark, config) =>
        val bucketName = "compactquerycheck"
        val numberParquetFiles = "5"
        val compactionInterval = config.compactionRecordInterval
        val configPath = getClass.getResource("/application.conf").toString.substring(5)
        val fs = SparkUtils.buildHadoopFs(config)
        val compactor = new TableFilesCompactor(spark, config)
        val queryExecutor = MetadataQueryExecutor(spark, config)
        val queryPizza = MetadataQuery(bucketName, """ userMd.`x-amz-meta-food` LIKE "pizza" """, None, 1000)
        val queryPasta = MetadataQuery(bucketName, """ userMd.`x-amz-meta-food` LIKE "pasta" """, None, 1000)
        val querySalad = MetadataQuery(bucketName, """ userMd.`x-amz-meta-food` LIKE "salad" """, None, 1000)

        // populate
        LandingMetadataPopulatorTool.main(Array(configPath, bucketName, "1000", numberParquetFiles))

        // premake staging file
        fs.mkdirs(new Path(config.stagingPathUri))

        // check that landing files were created
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldEqual numberParquetFiles.toInt

        // query should be successful before compaction
        val resultPizzaBefore = queryExecutor.execute(queryPizza)
        val resultPastaBefore = queryExecutor.execute(queryPasta)
        (resultPizzaBefore.count() + resultPastaBefore.count()) shouldBe 1000
        // should return nothing for non-existent metadata
        val resultSaladBefore = queryExecutor.execute(querySalad)
        resultSaladBefore.count() shouldBe 0

        // compact
        compactor.compactLandingPartition("bucket", bucketName, 1, false)

        // check landing (should be empty)
        fs.listStatus(new Path(config.landingPathUri, s"bucket=$bucketName"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // query again should be successful after compaction
        val resultPizzaAfter = queryExecutor.execute(queryPizza)
        val resultPastaAfter = queryExecutor.execute(queryPasta)
        (resultPizzaAfter.count() + resultPastaAfter.count()) shouldBe 1000
        // query again to make sure that non-existent metadata results still DNE
        val resultSaladAfter = queryExecutor.execute(querySalad)
        resultSaladAfter.count() shouldBe 0

        // should now have a staging object
        fs.listStatus(new Path(config.stagingPathUri, s"bucket=$bucketName/maxOpIndex=$compactionInterval"),
          SparkUtils.parquetFilesFilter).length shouldBe 1
    }
  }
}