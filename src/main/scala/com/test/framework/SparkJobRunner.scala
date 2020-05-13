package com.test.framework

import com.test.framework.common.{ApplicationContextProperties, ApplicationLog, Context}
import com.test.framework.common.Constants._
import com.test.framework.consumer.ConsumerFactory
import com.test.framework.enums.{ConsumerEnum, ProducerTypeEnum}
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.apache.log4j.Logger

object SparkJobRunner {
  val log: Logger = Logger.getLogger(getClass.getName)
  val commandUsage = "Example usage: SparkJobRunner targetTableName"
  var targetTable: String = _
  def main(args: Array[String]): Unit = {
    if(args.length <= 0){
      log.info(s"argument length ${args.length}")
      ApplicationLog.insertApplicationLog("Wrong Number Arguments" , "2STAG")
      ApplicationLog.updateApplicationLog("FAILED")
      log.error(s"Wrong Number arguments for application. $commandUsage")
      System.exit(1)
    }

    try{
      loadSparkJobConfig(args)
    }catch {
      case ex@(_: ConfigException | _: NoSuchElementException) => val errorMsg = ex.getLocalizedMessage.split(":")
        println(s"Error Spark Job Configuration: ${errorMsg(errorMsg.length - 1)}")
        System.exit(1)
    }
    implicit val spark = Context.getSparkSession()

    try{
      log.info(s"Starting loading process: ${ApplicationContextProperties.process}")
      val producer = ProducerFactory.getProducer(ApplicationContextProperties.producerType, targetTable)
      val consumer = ConsumerFactory.getConsumer(ApplicationContextProperties.consumerType, targetTable)
      producer.loadDataFromSource(consumer)
    }
    catch {
      case exception: Exception => {
        ApplicationLog.updateApplicationLog("FAILED")
        log.error(exception)
      }
    } finally {
      log.info("Ends the loading process")
    }
  }

  def loadSparkJobConfig(args: Array[String]): Unit = {
    val conf = ConfigFactory.load()

    targetTable = args(0)
    ApplicationContextProperties.runMode = conf.getString(RUN_MODE)
    ApplicationContextProperties.maxNumberDBConnections = conf.getInt(MAX_NUMBER_DB_CONNECTIONS_PROPERTIES)
    ApplicationContextProperties.rddRepartitionSize = conf.getInt(RDD_REPARTITION_SIZE_PROPERTIES)
    ApplicationContextProperties.fetchSize = conf.getInt(MAX_FETCH_SIZE_DB_PROPERTIES)

    ApplicationContextProperties.consumerType = ConsumerEnum.withName(conf.getString(CONSUMER_TYPE_PROPERTIES))
    if(args.length > 1){
      ApplicationContextProperties.producerType = ProducerTypeEnum.withName(args(1))
    } else {
      ApplicationContextProperties.producerType = ProducerTypeEnum.withName(conf.getString(PRODUCER_TYPE_PROPERTIES))
    }
    ApplicationContextProperties.process = s"$APPLICATION_LAND2RAWV.${targetTable}"
  }

}
