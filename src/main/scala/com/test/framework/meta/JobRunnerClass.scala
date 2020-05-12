package com.test.framework.meta
import java.util.concurrent.CountDownLatch

import com.test.framework.common.Constants._
import com.test.framework.common.{ApplicationContextProperties, ApplicationLog}
import com.test.framework.consumer.ConsumerFactory
import com.test.framework.enums.{ConsumerEnum, ProducerTypeEnum}
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

class JobRunnerClass {
  val log: Logger = Logger.getLogger(getClass.getName)
  val commandUsage = "Example usage: SparkJobRunner targetTableName"
  var targetTable: String = ""

  def run(target: String, latch: CountDownLatch)(
      implicit spark: SparkSession): Unit = {
    try {
      loadSparkJobConfig(target)
    } catch {
      case ex @ (_: ConfigException | _: NoSuchElementException) =>
        val errorMsg = ex.getLocalizedMessage.split(":")
        println(
          s"Error Spark Job Configuration: ${errorMsg(errorMsg.length - 1)}")
        sys.exit(1)
    }

    try {
      log.info(
        s"Starting loading process: ${ApplicationContextProperties.process}")
      log.info(spark.conf.getAll)
      val producer = ProducerFactory.getProducer(
        ApplicationContextProperties.producerType,
        targetTable)
      val consumer = ConsumerFactory.getConsumer(
        ApplicationContextProperties.consumerType,
        targetTable)
      producer.loadDataFromSource(consumer)
      log.info(
        s"Reduced the latch count, current latch count is ${latch.getCount}")
    } catch {
      case exception: Exception => {
        ApplicationLog.updateApplicationLog("FAILED")
        log.error(exception)
      }
    } finally {
      log.info("Ends the loading process")
      latch.countDown()
    }
  }

  def loadSparkJobConfig(args: String): Unit = {
    targetTable = args

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
      s"$APPLICATION_LAND2RAWV.${targetTable}"

  }

}
