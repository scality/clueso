package com.scality.clueso.merge

import java.util.UUID

import com.scality.clueso.{CluesoConfig, CluesoConstants}
import org.apache.spark.sql.SparkSession

object ParquetFileMerger {
  def merge(config : CluesoConfig, spark : SparkSession, path : String) = {
    val mergeOutputId = UUID.randomUUID()
    val outputPath = config.mergePath + "/" + mergeOutputId

    val data = spark.read
        .schema(CluesoConstants.eventSchema)
        .parquet(path)

    val numRecords = data.count
    val numFinalFiles = Math.floor(numRecords / config.mergeFactor).toInt + 1

    data.coalesce(numFinalFiles)
      .write
      .partitionBy("bucket")
      .parquet(outputPath)
  }

}
