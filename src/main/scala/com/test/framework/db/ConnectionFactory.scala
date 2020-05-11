package com.test.framework.db

import java.sql.{Connection, DriverManager}

import com.test.framework.common.ApplicationContextProperties
import com.test.framework.config.DatabaseConfig
import com.test.framework.configloader.ConfigLoaderFactory
import org.apache.log4j.Logger

object ConnectionFactory {
  //this parameter must be configure in the properties
  val log: Logger = Logger.getLogger(getClass.getName)
  private var connectionPool: List[Connection] = Nil
  private var dbProperties: DatabaseConfig = _

  /**
    * Create a new connection if the the number of connection is < maxDBConnections
    * it also insert this new connection in a List so the way to close a connection
    * is to call ConnectionFactory.closeConnection
    */
  def getConnection(
      connectionEnum: DBConnectionEnum.Value): Option[Connection] = {
    var connection: Connection = null
    if (dbProperties == null)
      loadConfig()

    if (connectionPool.size >= ApplicationContextProperties.maxNumberDBConnections) {
      log.error("Max Number of Open Connections reached")
      None
    } else {
      try {
        val (driver, dburl, username, password) = getConnection(connectionEnum)
        Class.forName(driver)
        connection = DriverManager.getConnection(dburl, username, password)
        connectionPool = connection :: connectionPool
        Option(connection)
      } catch {
        case ex: Exception => {
          log.error(s"Error couldnt create a new connection: ${ex}")
          None
        }
      }
    }
  }

  def closeConnection(connection: Connection): Unit = {
    if (connectionPool.contains(connection)) {
      connectionPool.find(_ eq connection).get.close()
      connectionPool = connectionPool.dropWhile(_ eq connection)
    }
  }

  private def loadConfig(): Unit = {
    val genericConfig =
      ConfigLoaderFactory.getConfig(ConfigEnum.DATABASE_CONFIG).orNull
    if (genericConfig != null)
      dbProperties = genericConfig.asInstanceOf[DatabaseConfig]
  }

  /**
    * Method to map the different connection to DB
    */
  private def getConnectionData(connectionEnum: DBConnectionEnum.Value)
    : (String, String, String, String) = {
    connectionEnum match {
      case DBConnectionEnum.DATARAW_CONNECTION =>
        (dbProperties.driverDbMetadata,
         dbProperties.urlDbMetadata,
         dbProperties.userNameDbMetadata,
         dbProperties.passwordDbMetadata)
      case DBConnectionEnum.METADATA_CONNECTION =>
        (dbProperties.driverDbMetadata,
         dbProperties.urlDbMetadata,
         dbProperties.userNameDbMetadata,
         dbProperties.passwordDbMetadata

        )
    }
  }
}
