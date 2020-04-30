package config

import com.test.framework.common.{Constants, Utility}

class KafkaConfig extends GenericConfig {
  var kafkaBootstrapServers: String = _
  var subscribeType: String = _
  var startingOffsets: String = _
  var maxOffsetsPerTrigger: String = _
  var kafkaSecurityProtocol: String = _
  var kafkaSslTruststoreLocation: String = _
  var kafkaSslTruststorePassword: String = _
  var kafkaSslKeystoreLocation: String = _
  var kafkaSslKeystorePassword: String = _
  var kafkaSslKeyPassword: String = _
  var kafkaTopics: List[String] = _
  var triggerDuration: List[String] = _
  var checkPointDirectory: List[String] = _
  var keySerializer: String = _
  var keyDeserializer: String = _
  var valueSerializer: String = _
  var valueDeserializer: String = _

  override def attributesToMap(): Map[String, String] = {
    var attributeMap = Map[String, String]()
    getClass.getDeclaredFields.foreach(f => {
      if (f.getType.getSimpleName == "String") {
        val attributeName = Utility.camelToPointCase(f.getName)
        val attributeValue = getClass.getMethods
          .find(_.getName == f.getName)
          .get
          .invoke(this)
          .asInstanceOf[String]
        if (attributeValue != null && !attributeName.isEmpty) {
          attributeName match {
            case nameKafka
                if attributeName.startsWith(Constants.KAFKA_PREFIX) => {
              attributeMap += (attributeName -> attributeValue)
              attributeMap += (nameKafka.substring(
                Constants.KAFKA_PREFIX.length) -> attributeValue)
            }

            case _ if attributeName == Constants.MAX_OFFSET_PER_TRIGGER => {
              attributeMap += (Constants.SPARK_MAX_OFFSET_NAME -> attributeValue)
              attributeMap += (Constants.KAFKA_CLIENT_MAX_OFFSET_NAME -> attributeValue)
            }
            case _ => attributeMap += (attributeName -> attributeValue)
          }
        }
      }
    })
    attributeMap
  }

}
