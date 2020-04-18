package com.test.framework.common

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

object Context {

  lazy val spark = {
    if(ApplicationContextProperties.runMode == "local") SparkSession.builder().master("local").getOrCreate()
    else SparkSession.builder().getOrCreate()
  }

  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)


}
