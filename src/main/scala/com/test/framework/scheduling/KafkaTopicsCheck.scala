package com.test.framework.scheduling

import java.nio.file.Paths
import java.nio.file.Files
import java.text.SimpleDateFormat

import com.test.framework.common.{Constants, OffsetUtility}
import org.apache.log4j.Logger
import com.test.framework.config.{KafkaConfig, KafkaTopicConfig, ListGenericConfig, MetadataHubConfig, MetadataLinkConfig}
import com.test.framework.configloader.ConfigLoaderFactory
import com.test.framework.consumer.KafkaConsumer
import com.test.framework.enums.{ConfigEnum, DBConnectionEnum}

object KafkaTopicsCheck {
  val targetTopicsOffset: List[KafkaTopicConfig] =
    ConfigLoaderFactory
      .getConfig(ConfigEnum.KAFKA_TOPIC_CONFIG,
                 None,
                 Option(DBConnectionEnum.DATARAW_CONNECTION))
      .get
      .asInstanceOf[ListGenericConfig]
      .listProperties
      .asInstanceOf[List[KafkaTopicConfig]]
  val log: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val path = args(0)
    val consumerProperties: Map[String, String] = ConfigLoaderFactory
      .getConfig(ConfigEnum.KAFKA_CONFIG)
      .get
      .asInstanceOf[KafkaConfig]
      .attributesToMap()
    val consumer = new KafkaConsumer[String, String](consumerProperties)
    //compare which kafka topics got new data
    //

    val listTargetTopic = getListTargetTopic()
    val updatedTargets = listTargetTopic
      .map(x => {
        val target = x._1
        val topic = x._2
        // log.info(s"Checking the pair $target, $topic")
        val committedOffset = getLatestCommitedOffset(target, topic)
        val kafkaTopicOffset =
          OffsetUtility.getKafkaTopicOffset(topic, consumer)
        //log.info(s"for table $target commited offset: $committedOffset, kafka offset: $kafkaTopicOffset")
        if (!committedOffset
              .equals(kafkaTopicOffset) && kafkaTopicOffset.nonEmpty) {
          //log.info(s""found discrepancy for $topic and $target)
          target
        } else None
      })
      .distinct
      .filter(_ != None)
      .map(x => x.toString)
    log.info(
      s"Found the following target tables with new records: $updatedTargets")
    writeTablesToFile(updatedTargets, path)
  }

  def getListTargetTopic(): List[(String, String)] = {

    val metadataHubList = ConfigLoaderFactory
      .getConfig(ConfigEnum.METADATA_HUB_CONFIG)
      .get
      .asInstanceOf[ListGenericConfig]
      .listProperties
      .asInstanceOf[List[MetadataHubConfig]]
      .map(x => (x.hubTable, x.sourceTopic))
      .distinct

    val metadataLinkList = ConfigLoaderFactory
      .getConfig(ConfigEnum.METADATA_LINK_CONFIG)
      .get
      .asInstanceOf[ListGenericConfig]
      .listProperties
      .asInstanceOf[List[MetadataLinkConfig]]
      .map(x => (x.linkTable, x.sourceTopic))
      .distinct

    val metadataSatList = ConfigLoaderFactory
      .getConfig(ConfigEnum.METADATA_SAT_CONFIG)
      .get
      .asInstanceOf[ListGenericConfig]
      .listProperties
      .asInstanceOf[List[MetadataSATConfig]]
      .map(x => (x.satTable, x.sourceTopic))
      .distinct

    val targetTopicList = metadataHubList ::: metadataLinkList ::: metadataSatList
    log.info(targetTopicList)
    targetTopicList
  }

  def getLatestCommitedOffset(target: String, topic: String): Map[Int, Long] = {
    val parser: SimpleDateFormat = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss.SSSSSS")
    val filteredConfigList = targetTopicsOffset
      .filter(
        _.process.toUpperCase == s"${Constants.APPLAYER_LAND2RAWV}.$target"
          .toUpperCase())
      .filter(_.source_topic.toUpperCase() == topic.toUpperCase())
    if (filteredConfigList.nonEmpty) {
      val filteredFormattedConfigList = filteredConfigList.map(x => {
        x.created_at = parser.format(parser.parse(x.created_at))
      })
      val maxDate: String = parser.format(
        filteredFormattedConfigList.map(x => parser.parse(x.created_at)).max)
      val offset = filteredFormattedConfigList
        .filter(_.created_at.toString == maxDate)
        .head
        .commit_offset
      OffsetUtility.mapFromOffsetString(offset)
    } else Map[Int, Long]()
  }
  def writeTablesToFile(tableList: List[String], path: String): Unit = {
    val content = tableList.mkString(" ").getBytes
    log.info(s"writing results into $path")
    Files.write(Paths.get(path), content)
  }
}
