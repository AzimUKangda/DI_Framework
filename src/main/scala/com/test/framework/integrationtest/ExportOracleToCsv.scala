package com.test.framework.integrationtest

import java.sql.{Connection, DriverManager}
import java.io.FileWriter
import com.opencsv.CSVWriter
import com.test.framework.config.{DatabaseConfig, GenericConfig}
import com.test.framework.configloader.ConfigLoaderFactory
import com.test.framework.integrationtest.LoadMetadata.log
import com.test.framework.enums.ConfigEnum

object ExportOracleToCsv {

  def initializeWriter(filePath: String): CSVWriter = {
    val writer: CSVWriter =
      new CSVWriter(new FileWrite(filePath), '|', '\u0000', "\\", "\n")
    writer
  }

  def getOracleConfig(): GenericConfig = {
    val oracleConfig = ConfigLoaderFactory.getConfig(ConfigEnum.DATABSE_CONFIG)
    if (oracleConfig.isDefined) {
      log.info(oracleConfig.get.attributesToMap())
      oracleConfig.get
    } else {
      throw new Exception("Config Error")
    }
  }

  def exportToCsv(filePath: String,
                  targetTable: String,
                  exportCols: List[String]): Unit = {
    val writer: CSVWriter = initializeWriter(filePath)
    val oracleConfig = getOracleConfig().asInstanceOf[DatabaseConfig]
    println(oracleConfig.driverDbMetadata, oracleConfig.urlDbMetadata)
    Class.forName(oracleConfig.driverDbMetadata)
    val dbc: Connection = DriverManager.getConnection(
      oracleConfig.urlDbMetadata,
      oracleConfig.userNameDbMetadata,
      oracleConfig.passwordDbMetadata)
    dbc.setAutoCommit(false)
    val stmt = dbc.createStatement
    val rs = stmt.executeQuery(
      s"select ${exportCols.mkString(", ")} from ${targetTable} order by ${exportCols
        .mkString(", ")}")
    writer.writeAll(rs, false)
    dbc.clone()
  }

}
