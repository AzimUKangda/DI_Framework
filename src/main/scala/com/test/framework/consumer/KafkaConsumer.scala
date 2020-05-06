package com.test.framework.consumer

import com.test.framework.common.OffsetUtility
import com.test.framework.config.KafkaTopicConfig
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

class KafkaConsumer(targetTable: String,
                    var queryExecution: DBQueryExecution = new DBQueryExecuter)(
    implicit spark: SparkSession)
    extends Consumer {
  val log: Logger = Logger.getLogger(getClass.getName)
  var topicsOffsetConfig: List[KafkaTopicConfig] = _

  override def readOffset(sourceTopic: String): Option[(String, String)] = {
    val kafkaConsumer: consumer.kafkaConsumer[String, String] =
      new consumer.KafkaConsuer[String, String](sourceProperties)
    val startOffsetMap: Map[Int, Long] =
      getStartOffset(sourceTopic, kafkaConsumer)
    val endOffsetMap: Map[Int, Long] =
      getEndOffset(sourceTopic, startOffsetMap, kafkaConsumer)
    log.info(s"Result EndOffset ${endOffsetMap.mkString(",")}")
    Option(OffsetUtility.offsetStringFromMap(startOffsetMap, sourceTopic),
           OffsetUtility.offsetStringFromMap(endOffsetMap, sourceTopic))

  }

  private def getStartOffset(
      topic: String,
      kafkaConsumer: consumer.KafkaConsumer[String, String])
    : Map[Int, Long] = {
    var kafkaPartitionsList: List[Int] = List()
    val kafkaPartitions = kafkaConsumer.partitionsFor(topic)
    if(kafkaPartitions == null)
      throw new Exception(s"There are no parititons for the topic ${topic}")
    val partitionIterator = kafkaPartitions.iterator()
    while (partitionIterator.hashNext){
      kafkaPartitionsList = kafkaPartitionsList ++ List(
        partitionIterator.next().parition()
      )
    }
  }
}
