package com.test.framework.producer

import com.test.framework.enums.ProducerTypeEnum
import com.test.framework.producer.targets.{DVHubProducer, DVLinkProducer, DVSatelliteProducer, RawProducer}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object ProducerFactory {


  val log = Logger.getLogger(getClass.getName)
  def getProducer(producerType:ProducerTypeEnum.Value,targetTable:String)(implicit spark :SparkSession):Producer ={
    val dataProducer = producerType match {
      case ProducerTypeEnum.RAW => new RawProducer
      case ProducerTypeEnum.HUB_DATAVAULT => new DVHubProducer(targetTable)
      case ProducerTypeEnum.LINK_DATAVAULT => new DVLinkProducer(targetTable)
      case ProducerTypeEnum.SATELLITE_DATAVAULT => new DVSatelliteProducer(targetTable)
    }
    dataProducer.loadConfig()
    dataProducer
  }
}
