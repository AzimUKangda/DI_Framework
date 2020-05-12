package com.test.framework.integrationtest

import com.test.framework.integrationtest.LoadMetadata.log

object DummySparkJob {
  def run(targetTable: String): Unit = {
    log.info(s"Loading data into ${targetTable}")
  }

}
