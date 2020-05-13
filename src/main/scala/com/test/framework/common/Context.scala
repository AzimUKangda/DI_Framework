package com.test.framework.common

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object Context {

  val hdfs = FileSystem.get(getSparkSession().sparkContext.hadoopConfiguration)

  def getSparkSession(): SparkSession = {
    if (ApplicationContextProperties.runMode == "local")
      SparkSession.builder().master("local").getOrCreate()
    else SparkSession.builder().getOrCreate()
  }

}
