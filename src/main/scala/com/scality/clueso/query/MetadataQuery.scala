package com.scality.clueso.query

case class MetadataQuery(bucketName : String, sqlWhereExpr : String, start_key : Option[String], limit : Int = 100) {
  override def toString: String = s"[MetadataQuery bucket=${bucketName}, query=$sqlWhereExpr, start_key=$start_key, end=$limit]"
}