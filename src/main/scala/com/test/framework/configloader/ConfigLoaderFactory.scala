package com.test.framework.configloader

import com.test.framework.config.GenericConfig
import com.typesafe.config.ConfigFactory
import java.util.HashMap

import com.test.framework.common.Constants._
import com.test.framework.common.Utility
import org.apache.log4j.Logger

object ConfigLoaderFactory {
  val log: Logger = Logger.getLogger(getClass.getName)
  private var listProperties = Map[ConfigEnum.Value, GenericConfig]()
  private var mappingSources =
    Map[ConfigEnum.Value, Map[ConfSourceEnum.Value, ConfigLoader]]()

  def getConfig(config: ConfigEnum.Value,
                filter: Option[List[String]] = None,
                connEnum: Option[DBConnectionEnum.Value] = None)
    : Option[GenericConfig] = {
    if (mappingSources.isEmpty)
      loadConfig()
    if (!listProperties.contains(config))
      setConfigFromMap(config, filter, connEnum)
    listProperties.get(config)
  }

  /**
    * Loads the mappingSource based on the application.conf, setting the ConfigSource and its possible
    * based in the type specified in the properties(simple of Complex)
    */
  private def loadConfig(): Unit = {
    log.info("Generating mapping from application config")
    //First we load the mapping for the FILE or DB config
    val conf = ConfigFactory.load()
    val rootConfIterator =
      conf.getObject(MAPPING_PROPERTIES).entrySet().iterator()
    while (rootConfIterator.hasNext) {
      val objectMap = rootConfIterator.next()
      try {
        val nodeConfig = objectMap.getValue.unwrapped()
        val nodeHashMap = nodeConfig.asInstanceOf[HashMap[String, AnyRef]]
        var sourceMapping = Map[ConfSourceEnum.Value, ConfigLoader]()
        for ((nodeKey, nodeValue) <- nodeHashMap) {
          val mappingSourceKey = ConfSourceEnum.withName(nodeKey.toUpperCase)
          nodeValue match {
            case nodeValue: String =>
              sourceMapping += (mappingSourceKey -> getSpecificProvider(
                mappingSourceKey,
                nodeValue))
            case nodeValue: HashMap[String, String] => {
              val source = nodeValue(SOURCE_NODE_CONFIG)
              val filterColumns = nodeValue(FILTER_COLUMNS_NODE_CONFIG)
              sourceMapping += (mappingSourceKey -> getSpecificProvider(
                mappingSourceKey,
                source,
                filterColumns.split(",").toList))
            }
          }
        }
        mappingSources += (ConfigEnum.withName(objectMap.getKey.toUpperCase()) -> sourceMapping)
      } catch {
        case exception: Exception =>
          log.error(s"Error: Could not load config mapping $exception")
          throw exception
      }
    }
  }

  /**
    * This method calls the loadConfig method of the specific source of data ex: DBConfigLoader, FileConfigLoader.
    * based on the mapping defined in the application config, and caches the result in the Map of GenericConfigs
    * in order to avoid loading the same configuration several times if requested in multiple parts of the program
    *
    */
  private def setConfigFromMap(
      config: ConfigEnum.Value,
      filter: Option[List[String]] = None,
      connEnum: Option[DBConnectionEnum.Value]): Unit = {
    try {
      log.info("Getting specific config for " + config)
      val classToCast = Class
        .forName(
          s"$PROPERTIES_PACKAGE.${Utility.underscoreToCamel(config.toString.toLowerCase).capitalize}")
        .newInstance()
        .asInstanceOf[GenericConfig]
      val configMap = mappingSources.get(config)
      if (configMap.isDefined) {
        configMap.get.foreach(f => {
          if (connEnum.isDefined) {
            f._2.asInstanceOf[DBConfigLoader].connEnum = connEnum.get
          }
          listProperties += (config -> f._2.loadConfig(filter, classToCast))
        })
      }
    } catch {
      case exception: Exception => log.error("Error:" + exception)
    }
  }

  private def getSpecificProvider(
      sourceType: ConfSourceEnum.Value,
      source: String,
      filterColumns: List[String] = List[String]()): ConfigLoader = {
    sourceType match {
      case ConfSourceEnum.DATABASE => new DBConfigLoder(source, filterColumns)
      case ConfSourceEnum.FILE     => new FileConfigLoader(source, filterColumns)
    }
  }
}
