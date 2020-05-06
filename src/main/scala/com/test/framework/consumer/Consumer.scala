package com.test.framework.consumer

import org.apache.spark.sql.DataFrame

trait Consumer {
  var sourceProperties: Map[String, String] = _

  def readOffset(source: String): Option[(String, String)]

  def writeOffset(source: String, offset: String, auditId: String): Unit

  def readDataFromSource(sourceOfData: String,
                         startOffset: String,
                         endOffset: String): DataFrame

  @throws(classOf[Exception])
  def loadConfig(): Unit

}
