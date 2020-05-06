package com.test.framework.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.SparkSession

object ConsumerFactory {

  /**
    * Method used to build a new Consumer base on the DataSource Enum.
    * It is responsible of instantiate and call the loading method, this helps
    * to maintain a unique class to create multiple dataSource, so if we create a new one
    * all the new DataSources should be child of consumer
    */
  def getConsumer(
      dataSourceType: ConsumerEnum.Value,
      targetConfigFilter: String)(implicit spark: SparkSession): Consumer = {
    //incase we need to load specific configuration we need to extend this part
    val dataSource = dataSourceType match {
      case ConsumerEnum.KAFKA => new KafkaConsumer(targetConfigFilter)
      // not yet implemented
      //case DataSourceEnum.HDFS => new HDFSSource
    }
    dataSource.loadConfig()
    dataSource
  }
}
