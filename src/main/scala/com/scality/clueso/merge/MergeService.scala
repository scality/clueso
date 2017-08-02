package com.scality.clueso.merge

import com.scality.clueso.CluesoConfig
import org.quartz._
import org.quartz.impl.StdSchedulerFactory

class MergeService(config : CluesoConfig) {
  lazy val quartz = StdSchedulerFactory.getDefaultScheduler

  val trigger: Trigger = TriggerBuilder
    .newTrigger
    .withIdentity("MergeTrigger", "Triggers")
    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
      .withIntervalInMilliseconds(config.mergeFrequency.toMillis))
    .build

  val job = JobBuilder.newJob(classOf[MergeParquetFilesJob])
    .withIdentity("MergeParquetFilesJob", "Jobs")
    .build

  quartz.start
  quartz.scheduleJob(job, trigger)


  class MergeParquetFilesJob extends Job {
    override def execute(jobExecutionContext: JobExecutionContext) = {
      println("Check merge conditions")
    }
  }
}
