package com.scality.clueso.query

case class MetadataQuery(bucketName : String, sqlWhereExpr : String, start : Int, end : Int) {
  override def toString: String = s"[MetadataQuery bucket=${bucketName}, query=$sqlWhereExpr, start=$start, end=$end]"
}