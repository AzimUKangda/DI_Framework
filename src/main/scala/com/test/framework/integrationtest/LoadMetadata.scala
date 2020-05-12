package com.test.framework.integrationtest

import java.sql.{Connection, DriverManager}

import com.test.framework.config.{DatabaseConfig, GenericConfig}
import com.test.framework.configloader.ConfigLoaderFactory
import com.test.framework.enums.ConfigEnum
import org.apache.log4j.{BasicConfigurator, Level, Logger}

import scala.io.Source

object LoadMetadata {
  val log: Logger = Logger.getLogger(getClass.getName)
  BasicConfigurator.configure()
  Logger.getRootLogger.setLevel(Level.INFO)

  def getOracleConfig(): GenericConfig = {
    val oracleConfig = ConfigLoaderFactory.getConfig(ConfigEnum.DATABASE_CONFIG)
    if (oracleConfig.isDefined) {
      log.info(oracleConfig.get.attributesToMap())
      oracleConfig.get
    } else {
      throw new Exception("Config Error")
    }
  }

  def getInsertStatements(filePath: String): List[String] = {
    //reading configs
    val insertStatementSource = Source.fromFile(filePath)
    val insertStatementList = insertStatementSource.getLines().toList
    insertStatementSource.clone()
    if (insertStatementList.nonEmpty) {
      insertStatementList
    } else {
      throw new Exception(s"File ${filePath} not found")
    }
  }

  def writeDmlToOracle(dmlFilePath: String): Unit = {
    val oracleConfig = getOracleConfig().asInstanceOf[DatabaseConfig]
    log.info(oracleConfig.attributesToMap())
    val sqlStatements = getInsertStatements(dmlFilePath)
    println(oracleConfig.driverDbMetadata, oracleConfig.urlDbMetadata)
    Class.forName(oracleConfig.driverDbMetadata)
    val dbc: Connection = DriverManager.getConnection(
      oracleConfig.urlDbMetadata,
      oracleConfig.userNameDbMetadata,
      oracleConfig.passwordDbMetadata)
    dbc.setAutoCommit(false)
    sqlStatements.foreach{ sqlString =>
      println(sqlString)
      dbc.prepareStatement(sqlString).executeUpdate()
      dbc.commit()
    }
    dbc.close()
    println(s"Inserted ${sqlStatements.size} records in database")
  }

}
