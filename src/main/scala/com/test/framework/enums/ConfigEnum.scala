package com.test.framework.enums

object ConfigEnum extends Enumeration {
  type ConfigEnum = Value
  //Properties for the DataSources
  val KAFKA_CONFIG, DATABSE_CONFIG = Value
  val METADATA_HUB_CONFIG, METADATA_LINK_CONFIG, METADATA_SAT_CONFIG = Value
  val SOURCE_SCHEMA_CONFIG = Value
  val INT_TEST_CONFIG, KAFKA_TOPIC_CONFIG = Value
  val METADATA_HUB_LIST_CONFIG, KAFKA_TOPIC_CONFIG_ALL, TABLE_TARGET_LIST =
    Value

}
