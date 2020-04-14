package com.test.framework.common

object Constants {

  val RUN_MODE = "run_mode"
  val PROPERTIES_PACKAGE = "com.test.framework.config"
  val MAPPING_PROPERTIES = "config_mapping"
  val MAX_NUMBER_DB_CONNECTIONS_PROPERTIES = "max_number_db_connections"
  val RDD_REPARTITION_SIZE_PROPERTIES = "rdd_repartition_size"
  val CONSUMER_TYPE_PROPERTIES = "consumer_type"
  val PRODUCER_TYPE_PROPERTIES = "producer_type"
  val MAX_FETCH_SIZE_DB_PROPERTIES = "max_fetch_size_db"
  val SOURCE_NODE_CONFIG = "source"
  val FILTER_COLUMNS_NODE_CONFIG = "filterColumns"
  val SPARK_APPLICATION_LOG_TABLE = "SPARK_JOB_DETAILS"
  val APPLICATION_LAND2RAWV = "Landing2RawV"
  val MAX_OFFSET_PER_TRIGGER = "max.offset.per.trigger"
  val SPARK_MAX_OFFSET_NAME = "maxOffsetPerTrigger"
  val KAFKA_CLIENT_MAX_OFFSET_NAME = "max.poll.records"
  val HASH_KEY_DELIMITER = raw"\@|"
  val ERROR_INFO_COL: String = "SABA_ERRORINFO"
  val VALIDFROM_TZ_COL = "SABA_GG_VALIDFROMTZ"
  val VALIDFROM_COL = "SABA_GG_VALIDFROM"
  val POS_COL = "SABA_GG_POS"
  val OPTYPE_COL = "SABA_GG_OPTYPE"
  val SABA_TOPICNAME = "SABA_TOPICNAME"
  val RAW_PATTERN = raw"RAW\((\d+)\)"
  val NUMBER_PATTERN = raw"NUMBER\((\d+)\)|NUMBER\((\d+),(\d+)\)|NUMBER"
  val INTERVALDS_PATTERN = raw"INTERVALDS\((\d+),(\d+)\)"
  val VARCHAR_PATTERN = raw"VARCHAR2\((\d+),(\d+)\)"
  val SABA_NULL_COL = "sabanull"
  val SABA_VALUE_COL = "value"
  val SOURCE_OFFSETTRACKING: String = "SABA_RAWVAULT_PROCESSTRACKING"
  val RECORDSOURCE = "RECORDSOURCE"
  val LOADDATE_COL = "LOADDATE"
  val AUDITID_COL = "AUDITID"
  val STATUS_SUCCESS = "SUCCEEDED"
  val IS_TRUE = "TRUE"
  val IS_FALSE = "FALSE"
  val KAFKA_PREFIX = "kafka."
  val UTC_TIMEFORMAT = "YYYY-MM-DD HH24:MI:SS.FF +THZ:TZM"
  val HEADER_LIST = List("service_name",
                         "schema",
                         "table",
                         "optype",
                         "timestamp",
                         "currenttimestamp",
                         "position",
                         "plant",
                         "product",
                         "area",
                         "source_system").map(_.toLowerCase)

}
