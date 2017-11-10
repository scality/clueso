package com.scality.clueso

object PathUtils {
  def landingURI(implicit config: CluesoConfig): String = config.landingPathUri
  def stagingURI(implicit config: CluesoConfig): String = config.stagingPathUri

}
