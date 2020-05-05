package com.test.framework.configloader
import com.test.framework.common.Utility
import com.test.framework.config.GenericConfig
import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
class FileConfigLoader(source: String, filterColumns: List[String])
    extends ConfigLoader(source, filterColumns) {
  val log: Logger = Logger.getLogger(getClass.getName)

  /**
    * This method loads the configuration base on a file specified in the source parameter.
    * Currently the filter option is not used but in the future if we want to filter some nodes of the configuration this
    * parameter could be used to specify which nodes to filter
    * It load the properties dinamically in the target classToCast Base on the names of the attributes, and the type
    * currently it only loads strings and List
   **/
  override def loadConfig[A <: GenericConfig](filter: Option[List[String]],
                                              classToCast: A): GenericConfig = {
    log.info(s"Generic Configuration for $source")
    val config = ConfigFactory.parseResources(source)
    if (config.isEmpty)
      throw new Exception(s"No properties to load from File $source")
    val classDefinition = classToCast.getClass
    classDefinition.getDeclaredFields.foreach(f => {
      if (config.hasPath(Utility.camelToPointCase(f.getName))) {
        val valueConf = config.getString(Utility.camelToPointCase(f.getName))
        try {
          f.getType.getSimpleName match {
            case "String" =>
              classDefinition.getMethods
                .find(_.getName == f.getName + "_$eq")
                .get
                .invoke(classToCast, valueConf)
            case "List" =>
              classDefinition.getMethods
                .find(_.getName == f.getName + "_$eq")
                .get
                .invoke(classToCast, valueConf.split(",").map(_.trim).toList)
          }
        } catch {
          case exception: Exception =>
            log.error(
              s"Error setting value: $valueConf to Field: ${f.getName}, $exception")
            throw exception
        }
      }
    })
    classToCast
  }
}
