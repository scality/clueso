package com.scality.clueso

import java.io.File
import java.sql.Timestamp

import com.scality.clueso.merge.TableFilesMerger
import com.scality.clueso.query.{MetadataQuery, MetadataQueryExecutor}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{Assertions, Matchers, WordSpec}

class CluesoMergingAndQueryingSpec extends WordSpec with Matchers with SparkContextSetup {
  "Metadata Queries" should {
    "Scenario 1: only retrieves the most recent PUT event for a given key" in withSparkContext {
      (spark, config) =>

        import spark.implicits._

        val fs = SparkUtils.buildHadoopFs(config)
        fs.mkdirs(new Path(config.stagingPath))

        val now = new java.util.Date().getTime

        val landingData = Seq(
          (new Timestamp(now), "{\"type\":\"put\",\"bucket\":\"wednesday\",\"key\":\"bigger\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":13,\"last-modified\":\"2017-08-08T03:57:02.249Z\",\"content-md5\":\"4b02d12ad7f063d67aec9dc2116a57a2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"12cb0b112d663e73effb32c58fe3fab9f4bd002c\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"}],\"isDeleteMarker\":false,\"tags\":{},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"userMd\":{\"x-amz-meta-verymeta\":\"2\",\"x-amz-meta-color\":\"blue\",\"x-amz-meta-dog\":\"retriever\",\"x-amz-meta-more\":\"morefun\",\"x-amz-meta-words\":\"runningout\",\"x-amz-meta-evenmore\":\"evenmorefun\",\"x-amz-meta-keepitup\":\"5\",\"x-amz-meta-mymeta1\":\"thisisfun\",\"x-amz-meta-mymeta2\":\"thisisfun2\"}}}"),
          (new Timestamp(now + 10000), "{\"type\":\"put\",\"bucket\":\"wednesday\",\"key\":\"fun\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":9829629,\"content-type\":\"text/plain\",\"last-modified\":\"2017-08-08T03:43:33.115Z\",\"content-md5\":\"1772d41ea77fc34588c131b057ab1ec3-2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"dcd2c7c3b34cb3975ab910e7fad6fe37dbd58654\",\"size\":5242880,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:496fe4b97d716800d77cf36c4f51cb72\"},{\"key\":\"850bc9cfe29cf0886b73778917329787500de2bd\",\"size\":4586749,\"start\":5242880,\"dataStoreName\":\"file\",\"dataStoreETag\":\"2:d30650f389fe4e164f5a6a6e7d6565b7\"}],\"tags\":{\"testing\":\"fun\",\"good\":\"night\"},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"userMd\":{\"x-amz-meta-s3cmd-attrs\":\"uid:1000/gname:scality/uname:scality/gid:1000/mode:33188/mtime:1493944455/atime:1495560440/md5:a18d867021e391c071ce1337bdc24b7f/ctime:1493944455\"}}}")
        )

        // when we have Landing with putA, putB, delA
        val landingDf = landingData.toDF("timestamp", "value")
        val stream = MetadataIngestionPipeline.filterAndParseEvents(config.bucketName, landingDf)
        stream.write
          .partitionBy("bucket")
          .parquet(config.landingPath)


        // TODO cache?
        val queryExecutor = MetadataQueryExecutor(spark, config)

        // given
        val query = new MetadataQuery("wednesday", """ userMd.`x-amz-meta-mymeta1` = 'thisisfun' """, None, 1000)
        var result = queryExecutor.execute(query)

        // then
        result.count() shouldBe 1

        var maybeB = result.take(1).head
        maybeB.getString(maybeB.fieldIndex("key")) shouldEqual "bigger"
    }

    "Scenario 2: remove entries when there's a newer event with TYPE = delete for same key" in withSparkContext {
      (spark, config) =>

        import spark.implicits._

        val fs = SparkUtils.buildHadoopFs(config)
        fs.mkdirs(new Path(config.stagingPath))

        val now = new java.util.Date().getTime

        val landingData = Seq(
          (new Timestamp(now), "{\"type\":\"put\",\"bucket\":\"wednesday\",\"key\":\"fun\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":13,\"last-modified\":\"2017-08-08T03:57:02.249Z\",\"content-md5\":\"4b02d12ad7f063d67aec9dc2116a57a2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"12cb0b112d663e73effb32c58fe3fab9f4bd002c\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"}],\"isDeleteMarker\":false,\"tags\":{},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"userMd\":{\"x-amz-meta-verymeta\":\"2\",\"x-amz-meta-color\":\"blue\",\"x-amz-meta-dog\":\"retriever\",\"x-amz-meta-more\":\"morefun\",\"x-amz-meta-words\":\"runningout\",\"x-amz-meta-evenmore\":\"evenmorefun\",\"x-amz-meta-keepitup\":\"5\",\"x-amz-meta-mymeta1\":\"thisisfun\",\"x-amz-meta-mymeta2\":\"thisisfun2\"}}}"),
          (new Timestamp(now + 10000), "{\"type\":\"put\",\"bucket\":\"wednesday\",\"key\":\"bigger\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":9829629,\"content-type\":\"text/plain\",\"last-modified\":\"2017-08-08T03:43:33.115Z\",\"content-md5\":\"1772d41ea77fc34588c131b057ab1ec3-2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"dcd2c7c3b34cb3975ab910e7fad6fe37dbd58654\",\"size\":5242880,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:496fe4b97d716800d77cf36c4f51cb72\"},{\"key\":\"850bc9cfe29cf0886b73778917329787500de2bd\",\"size\":4586749,\"start\":5242880,\"dataStoreName\":\"file\",\"dataStoreETag\":\"2:d30650f389fe4e164f5a6a6e7d6565b7\"}],\"tags\":{\"testing\":\"fun\",\"good\":\"night\"},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"userMd\":{\"x-amz-meta-s3cmd-attrs\":\"uid:1000/gname:scality/uname:scality/gid:1000/mode:33188/mtime:1493944455/atime:1495560440/md5:a18d867021e391c071ce1337bdc24b7f/ctime:1493944455\"}}}"),
          (new Timestamp(now + 20000), "{\"type\":\"delete\",\"bucket\":\"wednesday\",\"key\":\"fun\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":13,\"last-modified\":\"2017-08-08T03:57:02.249Z\",\"content-md5\":\"4b02d12ad7f063d67aec9dc2116a57a2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"12cb0b112d663e73effb32c58fe3fab9f4bd002c\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"}],\"isDeleteMarker\":false,\"tags\":{},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"userMd\":{\"x-amz-meta-verymeta\":\"2\",\"x-amz-meta-color\":\"blue\",\"x-amz-meta-dog\":\"retriever\",\"x-amz-meta-more\":\"morefun\",\"x-amz-meta-words\":\"runningout\",\"x-amz-meta-evenmore\":\"evenmorefun\",\"x-amz-meta-keepitup\":\"5\",\"x-amz-meta-mymeta1\":\"thisisfun\",\"x-amz-meta-mymeta2\":\"thisisfun2\"}}}")
        )

        // when we have Landing with putA, putB, delA
        val landingDf = landingData.toDF("timestamp", "value")
        val stream = MetadataIngestionPipeline.filterAndParseEvents(config.bucketName, landingDf)
        stream.write
          .partitionBy("bucket")
          .parquet(config.landingPath)


        // TODO cache?
        val queryExecutor = MetadataQueryExecutor(spark, config)


        // given
        val query = new MetadataQuery("wednesday", "", None, 1000)
        var result = queryExecutor.execute(query)


        // then
        result.count() shouldBe 1

        var maybeB = result.take(1).head
        maybeB.getString(maybeB.fieldIndex("key")) shouldEqual "bigger"

        // given we apply merge
        val merger = new TableFilesMerger(spark, config)
        merger.merge()

        // then, landing should be empty
        fs.listStatus(new Path(config.landingPath, "bucket=wednesday"),
          SparkUtils.parquetFilesFilter).length shouldEqual 0

        // given we query again (hits staging)
        result = queryExecutor.execute(query)

        // then same result as before
        result.count() shouldBe 1

        maybeB = result.take(1).head
        maybeB.getString(maybeB.fieldIndex("key")) shouldEqual "bigger"
    }

    "Scenario 3: not return entries in staging that are marked as deleted in landing" in withSparkContext {
      (spark, config) =>

        import spark.implicits._

        val now = new java.util.Date().getTime

        val landingData = Seq(
          (new Timestamp(now + 2000), "{\"type\":\"put\",\"bucket\":\"wednesday\",\"key\":\"fun\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":9829629,\"content-type\":\"text/plain\",\"last-modified\":\"2017-08-08T03:43:33.115Z\",\"content-md5\":\"1772d41ea77fc34588c131b057ab1ec3-2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"dcd2c7c3b34cb3975ab910e7fad6fe37dbd58654\",\"size\":5242880,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:496fe4b97d716800d77cf36c4f51cb72\"},{\"key\":\"850bc9cfe29cf0886b73778917329787500de2bd\",\"size\":4586749,\"start\":5242880,\"dataStoreName\":\"file\",\"dataStoreETag\":\"2:d30650f389fe4e164f5a6a6e7d6565b7\"}],\"tags\":{\"testing\":\"fun\",\"good\":\"night\"},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"userMd\":{\"x-amz-meta-s3cmd-attrs\":\"uid:1000/gname:scality/uname:scality/gid:1000/mode:33188/mtime:1493944455/atime:1495560440/md5:a18d867021e391c071ce1337bdc24b7f/ctime:1493944455\"}}}"),
          (new Timestamp(now + 1000), "{\"type\":\"delete\",\"bucket\":\"wednesday\",\"key\":\"fun\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":13,\"last-modified\":\"2017-08-08T03:57:02.249Z\",\"content-md5\":\"4b02d12ad7f063d67aec9dc2116a57a2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"12cb0b112d663e73effb32c58fe3fab9f4bd002c\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"}],\"isDeleteMarker\":false,\"tags\":{},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"userMd\":{\"x-amz-meta-verymeta\":\"2\",\"x-amz-meta-color\":\"blue\",\"x-amz-meta-dog\":\"retriever\",\"x-amz-meta-more\":\"morefun\",\"x-amz-meta-words\":\"runningout\",\"x-amz-meta-evenmore\":\"evenmorefun\",\"x-amz-meta-keepitup\":\"5\",\"x-amz-meta-mymeta1\":\"thisisfun\",\"x-amz-meta-mymeta2\":\"thisisfun2\"}}}")
        )

        val stagingData = Seq(
          (new Timestamp(now), "{\"type\":\"put\",\"bucket\":\"wednesday\",\"key\":\"fun\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":13,\"last-modified\":\"2017-08-08T03:57:02.249Z\",\"content-md5\":\"4b02d12ad7f063d67aec9dc2116a57a2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"12cb0b112d663e73effb32c58fe3fab9f4bd002c\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"}],\"isDeleteMarker\":false,\"tags\":{},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"userMd\":{\"x-amz-meta-verymeta\":\"2\",\"x-amz-meta-color\":\"red\",\"x-amz-meta-dog\":\"retriever\",\"x-amz-meta-more\":\"morefun\",\"x-amz-meta-words\":\"runningout\",\"x-amz-meta-evenmore\":\"evenmorefun\",\"x-amz-meta-keepitup\":\"5\",\"x-amz-meta-mymeta1\":\"thisisfun\",\"x-amz-meta-mymeta2\":\"thisisfun2\"}}}")
        )

        // when we have Staging with putA , Landing with delA, putB
        val stagingDf = stagingData.toDF("timestamp", "value")
        var stream = MetadataIngestionPipeline.filterAndParseEvents(config.bucketName, stagingDf)
        stream.write
          .partitionBy("bucket")
          .mode(SaveMode.Overwrite)
          .parquet(config.stagingPath)

        val landingDf = landingData.toDF("timestamp", "value")
        stream = MetadataIngestionPipeline.filterAndParseEvents(config.bucketName, landingDf)
        stream.write
          .partitionBy("bucket")
          .mode(SaveMode.Overwrite)
          .parquet(config.landingPath)

        // TODO cache?
        val queryExecutor = MetadataQueryExecutor(spark, config)

        // given
        val query = new MetadataQuery("wednesday", "", None, 1000)
        var result = queryExecutor.execute(query)

        // then
        result.count() shouldBe 1

        var maybeB = result.take(1).head
        maybeB.getString(maybeB.fieldIndex("key")) shouldEqual "fun"

        // given we apply merge
        val merger = new TableFilesMerger(spark, config)
        merger.merge()

        // then, landing should be empty
        val fs = SparkUtils.buildHadoopFs(config)
        fs.listStatus(new Path(config.landingPath, "bucket=wednesday"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // given we query again (hits staging)
        result = queryExecutor.execute(query)

        // then same result as before
        result.count() shouldBe 1

        maybeB = result.take(1).head
        maybeB.getString(maybeB.fieldIndex("key")) shouldEqual "fun"
    }

    "Scenario 4: pagination works" in withSparkContext {
      (spark, config) =>

        import spark.implicits._

        val now = new java.util.Date().getTime

        val landingData = Seq(
          (new Timestamp(now + 2000), "{\"type\":\"put\",\"bucket\":\"wednesday\",\"key\":\"fun\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":200,\"content-type\":\"text/plain\",\"last-modified\":\"2017-08-08T03:43:33.115Z\",\"content-md5\":\"1772d41ea77fc34588c131b057ab1ec3-2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"dcd2c7c3b34cb3975ab910e7fad6fe37dbd58654\",\"size\":5242880,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:496fe4b97d716800d77cf36c4f51cb72\"},{\"key\":\"850bc9cfe29cf0886b73778917329787500de2bd\",\"size\":4586749,\"start\":5242880,\"dataStoreName\":\"file\",\"dataStoreETag\":\"2:d30650f389fe4e164f5a6a6e7d6565b7\"}],\"tags\":{\"param\":\"yes\",\"good\":\"night\"},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"userMd\":{\"x-amz-meta-s3cmd-attrs\":\"uid:1000/gname:scality/uname:scality/gid:1000/mode:33188/mtime:1493944455/atime:1495560440/md5:a18d867021e391c071ce1337bdc24b7f/ctime:1493944455\"}}}"),
          (new Timestamp(now + 1000), "{\"type\":\"put\",\"bucket\":\"wednesday\",\"key\":\"fun2\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":100,\"last-modified\":\"2017-08-08T03:57:02.249Z\",\"content-md5\":\"4b02d12ad7f063d67aec9dc2116a57a2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"12cb0b112d663e73effb32c58fe3fab9f4bd002c\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"}],\"isDeleteMarker\":false,\"tags\":{\"param\":\"yes\"},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"userMd\":{\"x-amz-meta-verymeta\":\"2\",\"x-amz-meta-color\":\"blue\",\"x-amz-meta-dog\":\"retriever\",\"x-amz-meta-more\":\"morefun\",\"x-amz-meta-words\":\"runningout\",\"x-amz-meta-evenmore\":\"evenmorefun\",\"x-amz-meta-keepitup\":\"5\",\"x-amz-meta-mymeta1\":\"thisisfun\",\"x-amz-meta-mymeta2\":\"thisisfun2\"}}}")
        )

        val stagingData = Seq(
          (new Timestamp(now), "{\"type\":\"put\",\"bucket\":\"wednesday\",\"key\":\"fun3\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":13,\"last-modified\":\"2017-08-08T03:57:02.249Z\",\"content-md5\":\"4b02d12ad7f063d67aec9dc2116a57a2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"12cb0b112d663e73effb32c58fe3fab9f4bd002c\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"}],\"isDeleteMarker\":false,\"tags\":{},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"userMd\":{\"x-amz-meta-verymeta\":\"2\",\"x-amz-meta-color\":\"red\",\"x-amz-meta-dog\":\"retriever\",\"x-amz-meta-more\":\"morefun\",\"x-amz-meta-words\":\"runningout\",\"x-amz-meta-evenmore\":\"evenmorefun\",\"x-amz-meta-keepitup\":\"5\",\"x-amz-meta-mymeta1\":\"thisisfun\",\"x-amz-meta-mymeta2\":\"thisisfun2\"}}}")
        )

        // when we have Staging with putA , Landing with delA, putB
        val stagingDf = stagingData.toDF("timestamp", "value")
        var stream = MetadataIngestionPipeline.filterAndParseEvents(config.bucketName, stagingDf)
        stream.write
          .partitionBy("bucket")
          .mode(SaveMode.Overwrite)
          .parquet(config.stagingPath)

        val landingDf = landingData.toDF("timestamp", "value")
        stream = MetadataIngestionPipeline.filterAndParseEvents(config.bucketName, landingDf)
        stream.write
          .partitionBy("bucket")
          .mode(SaveMode.Overwrite)
          .parquet(config.landingPath)

        // TODO cache?
        val queryExecutor = MetadataQueryExecutor(spark, config)

        // given
        val query = new MetadataQuery("wednesday", """ tags.param="yes" """, None, 1)
        var result = queryExecutor.execute(query)

        // then
        result.count() shouldBe 1

        var maybeB = result.take(1).head
        var resultItemKey = maybeB.getString(maybeB.fieldIndex("key"))
        resultItemKey shouldEqual "fun"


        val page2Query = new MetadataQuery("wednesday", """ tags.param="yes" """, Some(resultItemKey), 1)
        result = queryExecutor.execute(page2Query)

        // then
        result.count() shouldBe 1

        maybeB = result.take(1).head
        resultItemKey = maybeB.getString(maybeB.fieldIndex("key"))
        resultItemKey shouldEqual "fun2"

        val page3Query = new MetadataQuery("wednesday", """ tags.param="yes" """, Some(resultItemKey), 1)
        result = queryExecutor.execute(page3Query)

        // then
        result.count() shouldBe 0


        // given we apply merge
        val merger = new TableFilesMerger(spark, config)
        merger.merge()

        // then, landing should be empty
        val fs = SparkUtils.buildHadoopFs(config)
        fs.listStatus(new Path(config.landingPath, "bucket=wednesday"), SparkUtils.parquetFilesFilter).length shouldEqual 0

        // given we query again (hits staging)
        result = queryExecutor.execute(query)

        // then same result as before
        result.count() shouldBe 1

        maybeB = result.take(1).head
        maybeB.getString(maybeB.fieldIndex("key")) shouldEqual "fun"
    }
  }
}

trait SparkContextSetup {
  def withSparkContext(testMethod: (SparkSession, CluesoConfig) => Any) {
    val parsedConfig = ConfigFactory.parseFile(new File(getClass.getResource("/application.conf").getFile))
    val _config = ConfigFactory.load(parsedConfig)

    val config = new CluesoConfig(_config)

    val spark = SparkUtils.buildSparkSession(config)
      .master("local[*]")
      .appName("Integration Test " + getClass.toString)
      .getOrCreate()

    // create dirs
    val fs = SparkUtils.buildHadoopFs(config)

    fs.delete(new Path(config.stagingPath), true)
    fs.delete(new Path(config.landingPath), true)

    try
      testMethod(spark, config)
    catch {
      case e: Exception =>
        e.printStackTrace()
        Assertions.fail(e)
    }
    finally {
      spark.stop()

      fs.delete(new Path(config.stagingPath), true)
      fs.delete(new Path(config.landingPath), true)
    }


  }
}