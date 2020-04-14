package com.test.framework.common

object ApplicationContextProperties {
  var targetTable: String = ""
  var runMode = ""
  var maxNumberDBConnections: Int = 10
  var rddRepartitionSize: Int = 3
  var process: String = ""
  var fetchSize
    : Int = 1000 //fetchsize option when writting to oracle. It can be tuned for better performance
  var producerType: ProducerTypeEnum.Value = ProducerTypeEnum.HUB_DATAVAULT
  var consumerType: ConsumerEnum.Value = ConsumerEnum.KAFKA

}
