package com.test.framework.enums

object MetadataEnum extends Enumeration {
  type ConfigEnum = Value

  val SOURCE_COLUMN, HASHKEY_COLUMN_ORDER, HASHKEY_COLUMN, BUSINESSKEY_COLUMN =
    Value
  val NON_REQUIRED_BUSINESSKEY, NON_REQUIRED_SOURCE_COLUMN, MULTI_ACTIVE_COL =
    Value

}
