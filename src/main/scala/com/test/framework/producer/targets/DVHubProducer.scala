package com.test.framework.producer.targets

import com.test.framework.config.ListGenericConfig
import com.test.framework.configloader.ConfigLoaderFactory
import com.test.framework.enums.ConfigEnum
import com.test.framework.producer.DVProducer
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.SortedMap
class DVHubProducer (override val target:String)(implicit spark:SparkSession) extends DVProducer(target) {

  @throws(classOf[Exception])
  override def loadConfig(): Unit = {
    super.loadConfig()
    log.info("star laoading HUB metadata")
    val configOption = ConfigLoaderFactory.getConfig(ConfigEnum.METADATA_HUB_CONFIG,Option(List(targetTable)))
    if(configOption.isDefined){
      val metadataHubList = configOption.get.asInstanceOf[ListGenericConfig]
      for(metadata <- metadataHubList.listProperties.asInstanceOf[ListGenericConfig]){
        metadataMap += (metadata.sourceTopic -> (metadata :: metadataMap.getOrElse(metadata.sourceTopic,List())))
      }
    }else
      throw new Exception("Error we could not load metadata from DVHubProducer")
  }

  

}
