package com.test.framework.integrationtest
import com.test.framework.config.{GenericConfig, KafkaConfig}
import com.test.framework.configloader.ConfigLoaderFactory
import com.test.framework.enums.ConfigEnum
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.mortbay.util.ajax.JSON.Source

object JsonFileToKafka {
  val log: Logger = Logger.getLogger(getClass.getName)
  BasicConfigurator.configure()
  Logger.getRootLogger.setLevel(Level.INFO)

  private def getConfigs(): GenericConfig = {
    val kafkaConfig = ConfigLoaderFactory.getConfig(ConfigEnum.KAFKA_CONFIG)
    if (kafkaConfig.isDefined)
      kafkaConfig.get
    else
      throw new Exception("Config Error")
  }

  def writeToKafkaTopic(jsonDataPath: String) = {
    val kafkaConfig = getConfigs().asInstanceOf[KafkaConfig]
    var messageCounter = 0
    val kafkaProducer: KafkaProducer[String, String] =
      new KafkaProducer[String, String](kafkaConfig.attributesToMap())
    // Sending data from json as Kafka Message
    val jsonInput = Source.fromFile(jsonDataPath)
    val kafkaTopics: List[String] = kafkaConfig.kafkaTopics
    jsonInput.getLines.foreach { data =>
      kafkaProducer.send(
        new ProducerRecord[String, String](kafkaTopics.head,
                                           kafkaTopics.head,
                                           data))
      messageCounter = messageCounter + 1
    }
    kafkaProducer.close()
    jsonInput.close()
    log.info(s"Total Data Pushed: $messageCounter")

  }

}
