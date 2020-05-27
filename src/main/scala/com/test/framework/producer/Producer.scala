package com.test.framework.producer

import java.sql.Connection

import com.test.framework.config.GenericConfig
import com.test.framework.consumer.Consumer

trait Producer {

  var dataProducerProperties:Option[GenericConfig] = None

  def loadDataFromSources(consumer:Consumer):Unit

  def writeDataToTarget(connection:Option[Connection]):Int

  @throws(classOf[Exception])
  def loadConfig():Unit

}
