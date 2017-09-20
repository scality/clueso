package com.scality.clueso.merge

import com.scality.clueso.CluesoConfig
import org.apache.spark.sql.SparkSession
import org.quartz._
import org.quartz.impl.StdSchedulerFactory

object MergeService {
  val JOB_CTX_TABLE_MERGER = "tableMerger"

  class MergeParquetFilesJob extends Job {
    def apply: MergeParquetFilesJob = new MergeParquetFilesJob()

    override def execute(context: JobExecutionContext) = {
      import org.quartz.{SchedulerContext, SchedulerException}

      var schedulerContext : SchedulerContext = null
      try
        schedulerContext = context.getScheduler.getContext
      catch {
        case e1: SchedulerException =>
          println("Failed getting quartz context")
          e1.printStackTrace()
      }

      val merger = schedulerContext.get(JOB_CTX_TABLE_MERGER).asInstanceOf[TableFilesMerger]

      println("Merge service execution")
      merger.merge()
    }
  }
}

class MergeService(spark : SparkSession, config : CluesoConfig) {
  import MergeService._

  val merger = new TableFilesMerger(spark, config)

  lazy val quartz = StdSchedulerFactory.getDefaultScheduler
  quartz.getContext.put(JOB_CTX_TABLE_MERGER, merger)

  val trigger: Trigger = TriggerBuilder
    .newTrigger
    .withIdentity("MergeTrigger", "Triggers")
    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
    .withIntervalInMilliseconds(config.mergeFrequency.toMillis))
    .build


  val job = JobBuilder.newJob(classOf[MergeParquetFilesJob])
    .withIdentity("MergeParquetFilesJob", "Jobs")
    .build

  quartz.start()
  quartz.scheduleJob(job, trigger)

  println("Merge service running")
}
