package com.scality.clueso

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode, TextNode}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}


class CluesoIngestionPipelineSpec extends WordSpec with Matchers with SparkContextSetup {
  "Metadata Pipeline" should {
    "Scenario 1: can rewrite user metadata properties JSON" in {
      val payload = """{"opIndex":"000006636351_000000","type":"put","bucket":"search10m","key":"4557700","value":"{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":0,\"content-type\":\"application/octet-stream\",\"last-modified\":\"2017-10-24T01:31:45.285Z\",\"content-md5\":\"d41d8cd98f00b204e9800998ecf8427e\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":null,\"isDeleteMarker\":false,\"tags\":{\"tagzzz1\":\"value1\",\"tagzzz2\":\"value2\"},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"x-amz-meta-color\":\"blue\",\"x-amz-meta-someotherkey\":\"someOtherMetadata4557700\"}"}"""
      val newPayload = MetadataIngestionPipeline.msgRewriteFun(payload.getBytes)

      val mapper = new ObjectMapper()
      val root = mapper.readTree(newPayload)

      Option(root.get("x-amz-meta-color")).isEmpty shouldEqual true
      val userMd = root.get("value").get("userMd").asInstanceOf[ObjectNode]
      Option(userMd).isDefined shouldEqual true
      Option(userMd.get("x-amz-meta-color")).isDefined shouldEqual true
      userMd.get("x-amz-meta-color").asInstanceOf[TextNode].textValue() shouldEqual "blue"
    }

    "Scenario 2: keep only the first Location of location arrays" in {
      val payload = """{"opIndex":"000006636351_000000","type":"put","bucket":"search10m","key":"4557700","value":"{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":0,\"content-type\":\"application/octet-stream\",\"last-modified\":\"2017-10-24T01:31:45.285Z\",\"content-md5\":\"d41d8cd98f00b204e9800998ecf8427e\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"1\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"},{\"key\":\"2\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:22222\"}],\"isDeleteMarker\":false,\"tags\":{\"tagzzz1\":\"value1\",\"tagzzz2\":\"value2\"},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"x-amz-meta-color\":\"blue\",\"x-amz-meta-someotherkey\":\"someOtherMetadata4557700\"}"}"""
      val newPayload = MetadataIngestionPipeline.msgRewriteFun(payload.getBytes)

      val mapper = new ObjectMapper()
      val root = mapper.readTree(newPayload)

      val location = root.get("value").get("location").asInstanceOf[ArrayNode]

      Option(location).isDefined shouldEqual true
      location.size() shouldEqual 1
      location.get(0).get("key").textValue() shouldEqual "1"
    }

    "Scenario 3: calculate the current max Op Index" in {
      val compactionRecordInterval = 10
      for (i <- 1 to compactionRecordInterval) {
        val result = MetadataIngestionPipeline.findNextMaxOpIndexFun(compactionRecordInterval, "%012d_0101".format(i))
        result shouldEqual compactionRecordInterval
      }

      for (i <- compactionRecordInterval + 1 to compactionRecordInterval * 2) {
        val result = MetadataIngestionPipeline.findNextMaxOpIndexFun(compactionRecordInterval*2, "%012d_0101".format(i))
        result shouldEqual compactionRecordInterval*2
      }
    }


    "Scenario 4: filters entries to PENSIEVE bucket" in {
      val parsedConfig = ConfigFactory.parseFile(new File(getClass.getResource("/application.conf").getFile))
      val loadedConfig = ConfigFactory.load(parsedConfig)

      val config = new CluesoConfig(loadedConfig)
      val spark = SparkUtils.buildSparkSession(config)
          .master("local[*]")
          .appName("Integration Test " + getClass.toString)
          .getOrCreate()
      import spark.implicits._
      implicit val _spark = spark
      implicit val _config = config

      val jsonStr = """{"opIndex":"000000001994_000000","type":"put","bucket":"PENSIEVE","key":"configuration/overlay-version","value":"{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":0,\"content-type\":\"application/octet-stream\",\"last-modified\":\"2017-10-24T01:31:45.285Z\",\"content-md5\":\"d41d8cd98f00b204e9800998ecf8427e\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"1\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"},{\"key\":\"2\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:22222\"}],\"isDeleteMarker\":false,\"tags\":{\"tagzzz1\":\"value1\",\"tagzzz2\":\"value2\"},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\",\"x-amz-meta-color\":\"blue\",\"x-amz-meta-someotherkey\":\"someOtherMetadata4557700\"}"}"""

      val landingData = Seq(jsonStr)
      val landingDf = landingData.toDF("value")
      val filteredDf = MetadataIngestionPipeline.filterAndParseEvents(config.bucketName, landingDf)
      filteredDf.count shouldEqual 0
      spark.stop()
    }

    "Scenario 5: filters events with incorrect value format" in {
      val payload = """{"opIndex":"000006636351_000000","type":"put","bucket":"search10m","key":"4557700","value":"1"}"}"""
      val newPayload = MetadataIngestionPipeline.msgRewriteFun(payload.getBytes)
      newPayload shouldEqual ""
    }
  }
}
