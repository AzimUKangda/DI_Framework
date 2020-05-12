package com.test.framework.meta
import java.util.concurrent.CountDownLatch

import com.test.framework.common.{ApplicationContextProperties, ApplicationLog}
import com.test.framework.common.Constants._
import com.test.framework.enums.{ConsumerEnum, ProducerTypeEnum}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object ParallelJobRunner {
  val log: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length <= 0) {
      log.info(s"argument length ${args.length}")
      ApplicationLog.insertApplicationLog("Wrong Number Argument", "2STAG")
      ApplicationLog.updateApplicationLog("FAILED")
      System.exit(1)
    }
    loadSparkJobConfig(args)
    implicit val spark = {
      if (ApplicationContextProperties.runMode == "local")
        SparkSession.builder().master("local").getOrCreate()
      else
        SparkSession.builder().getOrCreate()
    }
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair")
    val latch = new CountDownLatch(args.length)
    val tables = args
    for (table <- tables) {
      val thread = new Thread {
        override def run: Unit = {
          log.info(s"loading table $table")
          val sparkJobRunner = new JobRunnerClass
          sparkJobRunner.run(table, latch)
        }
      }
      thread.start()
    }
    latch.await()
    log.info("All Loading jobs ended")
  }

  def loadSparkJobConfig(args: Array[String]): Unit = {

    val conf = ConfigFactory.load()
    ApplicationContextProperties.runMode = conf.getString(RUN_MODE)
    ApplicationContextProperties.maxNumberDBConnections =
      conf.getInt(MAX_NUMBER_DB_CONNECTIONS_PROPERTIES)
    ApplicationContextProperties.rddRepartitionSize =
      conf.getInt(RDD_REPARTITION_SIZE_PROPERTIES)
    ApplicationContextProperties.fetchSize =
      conf.getInt(MAX_FETCH_SIZE_DB_PROPERTIES)
    ApplicationContextProperties.consumerType =
      ConsumerEnum.withName(conf.getString(CONSUMER_TYPE_PROPERTIES))
    ApplicationContextProperties.producerType =
      ProducerTypeEnum.withName(conf.getString(PRODUCER_TYPE_PROPERTIES))

    ApplicationContextProperties.process =
      s"$APPLICATION_LAND2RAWV.${args.mkString("")}"
    log.info(ApplicationContextProperties)
  }

}
