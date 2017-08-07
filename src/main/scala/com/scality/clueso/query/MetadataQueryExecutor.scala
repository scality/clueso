package com.scality.clueso.query

import java.io.File

import com.scality.clueso.CluesoConfig
import com.scality.clueso.merge.TableFilesMerger
import com.typesafe.config.ConfigFactory

class MetadataQueryJob(config: CluesoConfig) {


}

object MetadataQueryJob {
  def main(args: Array[String]): Unit = {
    require(args.length > 0, "specify configuration file")

    val parsedConfig = ConfigFactory.parseFile(new File(args(0)))
    val _config = ConfigFactory.load(parsedConfig)

    val config = new CluesoConfig(_config)

    val query = new MetadataQueryJob(config)
    val result = query.execute()
    println(result)
  }
}
