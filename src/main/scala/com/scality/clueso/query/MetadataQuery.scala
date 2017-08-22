package com.scality.clueso.query

import com.scality.clueso.CluesoConfig
import org.apache.spark.sql.SparkSession

case class MetadataQuery(spark : SparkSession, config: CluesoConfig, bucketName : String, sqlWhereExpr : String, start : Int, end : Int) {
  override def toString: String = s"[MetadataQuery bucket=${bucketName}, query=$sqlWhereExpr, start=$start, end=$end]"
}