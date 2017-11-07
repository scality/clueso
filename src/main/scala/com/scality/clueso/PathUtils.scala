package com.scality.clueso

object PathUtils {
  def landingURI(implicit config: CluesoConfig): String = config.landingPath
  def stagingURI(implicit config: CluesoConfig): String = config.stagingPath

}
