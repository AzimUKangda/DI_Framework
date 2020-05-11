package com.test.framework.enums

object ConsumerEnum extends Enumeration {
  type DataSourceEnum = Value
  val KAFKA, HDFS = Value
}
