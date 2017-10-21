package com.scality.clueso

import org.apache.spark.sql.Row
import org.scalatest.{Matchers, WordSpec}

//  buildJsonUserMetadataObject
class MetadataPipelineSpec extends WordSpec with Matchers {
  "Metadata Pipeline" should {
    "Aggregate S3 user metadata properties in a single nested object userMd" in {

      val payload = "{\"type\":\"put\",\"bucket\":\"aa\",\"key\":\"fuasdasdadn\",\"value\":{\"md-model-version\":3,\"owner-display-name\":\"CustomAccount\",\"owner-id\":\"12349df900b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47qwer\",\"content-length\":13,\"last-modified\":\"2017-08-08T03:57:02.249Z\",\"content-md5\":\"4b02d12ad7f063d67aec9dc2116a57a2\",\"x-amz-version-id\":\"null\",\"x-amz-server-version-id\":\"\",\"x-amz-storage-class\":\"STANDARD\",\"x-amz-server-side-encryption\":\"\",\"x-amz-server-side-encryption-aws-kms-key-id\":\"\",\"x-amz-server-side-encryption-customer-algorithm\":\"\",\"x-amz-website-redirect-location\":\"\",\"acl\":{\"Canned\":\"private\",\"FULL_CONTROL\":[],\"WRITE_ACP\":[],\"READ\":[],\"READ_ACP\":[]},\"key\":\"\",\"location\":[{\"key\":\"12cb0b112d663e73effb32c58fe3fab9f4bd002c\",\"size\":13,\"start\":0,\"dataStoreName\":\"file\",\"dataStoreETag\":\"1:4b02d12ad7f063d67aec9dc2116a57a2\"}],\"isDeleteMarker\":false,\"tags\":{},\"replicationInfo\":{\"status\":\"\",\"content\":[],\"destination\":\"\",\"storageClass\":\"\",\"role\":\"\"},\"dataStoreName\":\"us-east-1\", \"x-amz-meta-verymeta\":\"2\",\"x-amz-meta-color\":\"blue\",\"x-amz-meta-dog\":\"retriever\",\"x-amz-meta-more\":\"morefun\",\"x-amz-meta-words\":\"runningout\",\"x-amz-meta-evenmore\":\"evenmorefun\",\"x-amz-meta-keepitup\":\"5\",\"x-amz-meta-mymeta1\":\"thisisfun\",\"x-amz-meta-mymeta2\":\"thisisfun2\"}}"

      val result = MetadataIngestionPipeline.buildJsonUserMetadataObject(Row(payload))

      result.getString(0).contains("\"userMd\":{\"x-amz-meta-") shouldBe true
    }
  }

}
