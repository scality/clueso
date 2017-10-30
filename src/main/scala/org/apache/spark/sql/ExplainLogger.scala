package org.apache.spark.sql

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.execution.command.ExplainCommand

object ExplainLogger extends LazyLogging {

  def explain(sparkSession : SparkSession, result: Dataset[Row]) = {
    val explain = ExplainCommand(result.queryExecution.logical, extended = true)
    sparkSession.sessionState.executePlan(explain).executedPlan.executeCollect().foreach {
      r => logger.info(r.getString(0))
    }
  }
}
