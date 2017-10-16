package com.scality.clueso

import com.typesafe.scalalogging.LazyLogging

object AlluxioUtils extends LazyLogging {

  // alluxio settings set out here: http://www.alluxio.org/docs/master/en/Configuration-Properties.html
  // appears default cache is 1 hour? change to 60 seconds to match merge frequency?
  def landingURI(implicit config : CluesoConfig): String = {
    if (!config.cacheDataframes) {
      config.landingPath
    } else s"${config.alluxioUrl}${config.bucketLandingPath}"
  }

  def stagingURI(implicit config : CluesoConfig): String = {
    if (!config.cacheDataframes) {
      config.stagingPath
    } else s"${config.alluxioUrl}${config.bucketStagingPath}"
  }

}
