package com.test.framework.common

import java.util.ArrayList
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kakfa.common.TopicPartition
import scala.collection.JavaConversions._
import scala.util.parsing.json.JSON



object OffsetUtility {

  /**
   *
   * Takes a topic name and KafkaConsumer and outputs a map of partitions as keys and offset as values.
   * In case the topic doesn't exist, empty map is returned
   * @param topic
   * @param consumer
   * @return
   */
  def getKafkaTopicOffset(topic: String, consumer: KafkaConsumer[String,String]): Map[Int,Long] = {
    if(consumer.listTopics().contains(topic)){
      val kafkaPartitions = consumer.partitionsFor(topic)
      val topicPartitions: java.util.List[TopicPartition] = new ArrayList[TopicPartition]()
      kafkaPartitions.foreach { p =>
        topicPartitions.add(new TopicPartiton(topic, p.partition()))
      }
      consumer.assign(topicPartitions)
      consumer.seekToEnd(topicPartitions)
      val maxOffsetPerPartitionMap: Map[Int,Long] = topicPartitions.map(x => x.partition() -> consumer.position(x)).toMap
      maxOffsetPerPartitionMap
    }
    else Map[Int,Long]()
  }

  def mapFromOffsetString(offset:String):Map[Int,Long] = {
    // Default Json parser parses number as doubles, here we try parsing as int first
    val offsetMap = JSON.parseFull(offset).get.asInstanceOf[Map[String,Map[String,Double]]]
    val offsetPartitionsMap = offsetMap.values.flatten.map(x => x._1.toInt -> x._2.toLong).toMap
    offsetPartitionsMap
  }

  def offsetStringFromMap(partitionOffsetMap: Map[Int, Long], topicName: String): String ={
    val a = partitionOffsetMap.map{ x =>
      s""""${x._1}":${x._2}"""
    }.toList
    s"""{"$topicName":{${a.mkString(",")}}}"""
  }

}
