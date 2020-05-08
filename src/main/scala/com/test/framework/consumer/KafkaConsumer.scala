package com.test.framework.consumer

import java.text.SimpleDateFormat
import java.util.ArrayList

import com.test.framework.common.OffsetUtility
import com.test.framework.common.Constants._
import com.test.framework.config.{KafkaConfig,KafkaTopicConfig, ListGenericConfig}
import com.test.framework.configloader.ConfigLoaderFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.kafka.clients.consumer
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger

class KafkaConsumer(targetTable: String,
                    var queryExecution: DBQueryExecution = new DBQueryExecuter)(
    implicit spark: SparkSession)
    extends Consumer {

  val log: Logger = Logger.getLogger(getClass.getName)
  var topicsOffsetConfig: List[KafkaTopicConfig] = _

  override def readOffset(sourceTopic: String): Option[(String, String)] = {
    val kafkaConsumer: consumer.KafkaConsumer[String, String] =
      new consumer.KafkaConsumer[String, String](sourceProperties)
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
      kafkaConsumer: consumer.KafkaConsumer[String, String]): Map[Int, Long] = {
    var kafkaPartitionsList: List[Int] = List()
    val kafkaPartitions = kafkaConsumer.partitionsFor(topic)
    if (kafkaPartitions == null)
      throw new Exception(s"There are no parititons for the topic ${topic}")
    val partitionIterator = kafkaPartitions.iterator()
    while (partitionIterator.hashNext) {
      kafkaPartitionsList = kafkaPartitionsList ++ List(
        partitionIterator.next().parition()
      )
    }
    val committedOffset = readLatestCommitedOffset(topic)

    if (!committedOffset.forall(_.isEmpty)) {
      log.info(s"Last commitedoffset ${committedOffset.get}")
      val committedOffsetMap =
        OffsetUtility.mapFromOffsetString(committedOffset.get)
      log.info(s"getMapFrom JSONOffset ${committedOffsetMap.mkString(",")}")
      val newPartitionList = kafkaPartitionsList diff committedOffsetMap.keys.toList
      log.info(s"newPartitionList ${newPartitionList.mkString(",")}")
      committedOffsetMap ++ newPartitionList.map(x => x -> -2L)
    } else {
      kafkaPartitionsList.map(x => (x, -2L)).toMap
    }
  }

  def readLatestCommitedOffset(topic: String): Option[String] = {
    var offset = ""
    val parser: SimpleDateFormat = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss.SSSSSS")
    try {
      val filteredConfigList: List[KafkaTopicConfig] =
        topicsOffsetConfig.filter(_.source_topic == topic)
      val filteredFormattedConfigList = filteredConfigList.map(x => {
        x.created_at = parser.format(parser.parse(x.created_at))
      })
      val maxDate: String = parser.format(
        filteredFormattedConfigList.map(x => parser.parse(x.created_at)).max)
      offset = filteredFormattedConfigList
        .filter(_.created_at.toString == maxDate)
        .head
        .commit_offset
    } catch {
      case exception: Exception => log.error("NO COnfig was Found" + exception)
    }
    Option(offset)
  }

  def getEndOffset(
      topic: String,
      startOffset: Map[Int, Long],
      kafkaConsumer: consumer.KafkaConsumer[String, String]): Map[Int, Long] = {
    val topicPartitions: java.util.List[TopicPartition] =
      new ArrayList[TopicPartition]()
    startOffset.foreach { p =>
      topicPartitions.add(new TopicPartition(topic, p._1.toInt))
    }
    log.info(s"getEndOffset ${topicPartitions.mkString(",")}")
    kafkaConsumer.assign(topicPartitions)
    kafkaConsumer.seekToEnd(topicPartitions)
    val maxOffsetPerPartitionMap: Map[Int, Long] =
      topicPartitions.map(x => x.partition() -> kafkaConsumer.position(x)).toMap
    val startPartitionOffsetMap: Map[Int, Long] = startOffset.toList
      .map(
        x =>
          x._1 -> scala.math
            .min(x._2 + 1, maxOffsetPerPartitionMap.getOrElse(x._1, x._2 + 1L)))
      .toMap
    val diffOffsetPerPartitionMap: Map[Int, Long] =
      maxOffsetPerPartitionMap.toList
        .map(x => x._1 -> (x._2 - startPartitionOffsetMap.getOrElse(x._1, 0L)))
        .toMap
    val totalMaxOffset = diffOffsetPerPartitionMap.values.toList.sum

    if (totalMaxOffset != 0) {
      var maxOffset = 1000L
      if (sourceProperties.contains(KAFKA_CLIENT_MAX_OFFSET_NAME)) {
        maxOffset = sourceProperties(KAFKA_CLIENT_MAX_OFFSET_NAME).toLong
      }
      val unprocessedOffset = scala.math.min(totalMaxOffset, maxOffset)

      val rationOfMessagesMap: Map[Int, Long] = diffOffsetPerPartitionMap.toList
        .map(x => x._1 -> (x._2 * unprocessedOffset) / totalMaxOffset)
        .toMap

      val endPartitionOffsetMap: Map[Int, Long] =
        rationOfMessagesMap.toList
          .map(x =>
            x._1 -> (x._2 + startPartitionOffsetMap.getOrElse(x._1, 0L)))
          .toMap
      endPartitionOffsetMap
    } else {
      startPartitionOffsetMap
    }

  }

  def writeOffset(topic: String, offset: String, auditId: String): Unit = {
    val offsetUpdate = queryExecutor.createDynamicInsert(
      SOURCE_OFFSETTRACKING,
      List(s"''$topic",
           s"'$APPLICATION_LAND2RAWV.$targetTable'",
           s"'$offset'",
           "sys_extract_utc(current_timestamp)",
           s"'$auditId'"))
    log.info(s"Writing offset: $offsetUpdate")
    queryExecution.executeUpdate(offsetUpdate,
                                 DBConnectionEnum.DATARAW_CONNECTION)
  }

  def readDataFromSource(sourceData: String,
                         startOffset: String,
                         endOffset: String): DataFrame = {
    val rawDF = spark.read
      .format("kafka")
      .options(sourceProperties)
      .option("subscribe", sourceData)
      .option("startingOffsets", startOffset)
      .option("endingOffsets", endOffset)
      .load
      .selectExpr("CAST(key AS STRING)",
                  "CAST(value AS STRING)",
                  "topic",
                  "offset",
                  "partition")
    rawDF

  }

  @throws(classOf[Exception])
  def loadConfig(): Unit = {
    log.info(
      s"Getting configuration for KafkaConsumer, writing into $targetTable")
    val sourcePropertiesConfig =
      ConfigLoaderFactory.getConfig(ConfigEnum.KAFKA_CONFIG)
    val offsetConfig = ConfigLoaderFactory.getConfig(
      ConfigEnum.KAFKA_TOPIC_CONFIG,
      Option(List(s"$APPLICATION_LAND2RAWV.$targetTable")),
      Option(DBConnectionEnum.DATARAW_CONNECTION))
    if (sourcePropertiesConfig.isDefined && offsetConfig.isDefined) {
      sourceProperties =
        sourcePropertiesConfig.get.asInstanceOf[KafkaConfig].attributesToMap()
      topicsOffsetConfig = offsetConfig.get
        .asInstanceOf[ListGenericConfig]
        .listProperties
        .asInstanceOf[List[KafkaTopicConfig]]
    } else
      throw new Exception(s"Error loading configuration for KafkConsumer")
  }
}
