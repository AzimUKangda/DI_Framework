package com.test.framework.configloader

import com.test.framework.config.GenericConfig

abstract class ConfigLoader(var source: String,
                            var filterColumns: List[String]) {
  def loadConfig[A <: GenericConfig](filter: Option[List[String]],
                                     classToCast: A): GenericConfig
}
