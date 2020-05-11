package com.test.framework.db

import java.sql.{Connection, SQLException}

import com.test.framework.common.Utility
import com.test.framework.common.Constants._
import org.apache.log4j.Logger

class DBQueryExecutor {
  val log: Logger = Logger.getLogger(getClass.getName)

  def createDynamicQuery(columns: List[String],
                         table: String,
                         filters: List[String]): String = {
    var sqlStatement = s"SELECT ${columns.mkString(",")} FROM $table"
    if (filters.nonEmpty) {
      sqlStatement += s" WHERE ${filters.mkString("AND ")}".toUpperCase()
    }
    sqlStatement
  }

  def createDynamicInsert(table: String, insertValues: List[String]): String = {
    s"INSERT INTO $table VALUES (${insertValues.mkString(",")})".toUpperCase()
  }

  def createDynamicUpdate(table: String,
                          columnsValue: List[String],
                          filters: List[String]): String = {
    s"UPDATE $table SET ${columnsValue.mkString(",")} where ${filters.mkString(" AND ")}"
      .toUpperCase()
  }

  /**
    * Method to execute a Query and returns two parameters
    * 1) IndexedSeq of ColumnsNames
    * 2) List of IndexedSeq with the result of the query
    * In this ways the class that called *executeQuery* is responsible of handle the response, for example using reflection nd
    * dinamically fill a class
    */
  def executeQuery(query: String,
                   connEnum: DBConnectionEnum.Value =
                     DBConnectionEnum.METADATA_CONNECTION)
    : (Option[IndexedSeq[String]], Option[List[IndexedSeq[String]]]) = {
    val connection = ConnectionFactory.getConnection(connEnum)
    var columnsOp: Option[IndexedSeq[String]] = None
    var resultsOp: Option[List[IndexedSeq[String]]] = None
    if (connection.isDefined) {
      try {
        val statement = connection.get.prepareStatement(query)
        val rs = statement.executeQuery()
        val columns
          : IndexedSeq[String] = 1 to rs.getMetaData.getColumnCount map rs.getMetaData.getColumnName
        val results: List[IndexedSeq[String]] = Iterator
          .continually(rs)
          .takeWhile(_.next())
          .map { row =>
            columns map row.getString
          }
          .toList
        columnsOp = Option(columns)
        resultsOp = Option(results)
      } catch {
        case exception: Exception => {
          log.error(s"Error executing query " + exception)
          throw exception
        }
      } finally {
        ConnectionFactory.closeConnection(connection.get)
      }
    }
    (columnsOp, resultsOp)
  }

  def executeUpdate(query: String,
                    connEnum: DBConnectionEnum.Value =
                      DBConnectionEnum.METADATA_CONNECTION): Unit = {
    val connection = ConnectionFactory.getConnection(connEnum)
    if (connection.isDefined) {
      try {
        val statement = connection.get.createStatement()
        statement.executeUpdate(query)
      } catch {
        case exception: Exception => {
          log.error(s"Error executing update query " + exception)
          throw exception
        }
      } finally {
        ConnectionFactory.closeConnection(connection.get)
      }
    }
  }

  def lockTable(targetTable: String): Option[Connection] = {
    val connectionOp =
      ConnectionFactory.getConnection(DBConnectionEnum.DATARAW_CONNECTION)
    try {
      if (connectionOp.isDefined) {
        val connection = connectionOp.get
        connection.setAutoCommit(false)
        log.info(s"Locking Table $targetTable in Oracle Database")
        connection
          .createStatement()
          .executeQuery(s"LOCK TABLE $targetTable IN EXCLUSIVE MODE NOWAIT")
        log.info(s"Table $targetTable locked in Oracle Database")
      }
    } catch {
      case exception: Exception =>
        log.error(s"Error locking the table $targetTable" + exception)
    }
    connectionOp
  }

  /**
    * Gets de Metadata of a targetTable and retrieves a Map with:
    * Key -> ColumnName
    * Value -> Type
    *
    * And also a List of the column that are NotNullable in the targetTable
    */
  def getTableSchema(targetTable: String)
    : (Option[Map[String, String]], Option[List[String]]) = {
    val connection =
      ConnectionFactory.getConnection(DBConnectionEnum.DATARAW_CONNECTION)
    var schemaResult: Option[Map[String, String]] = None
    var notNullableColumns: Option[List[String]] = None
    if (connection.isDefined) {
      try {
        var targetColumnMeta: Map[String, String] = Map()
        var notNullColumnsList: List[String] = List()
        val columns =
          connection.get.getMetaData.getColumns(null, null, targetTable, null)
        while ({
          columns.next
        }) {
          val colTypeName = columns.getString("TYPE_NAME")
          val colName = columns.getString("COLUMN_NAME")
          val colScale = columns.getString("DECIMAL_DIGITS")
          val colPrecision = columns.getString("COLUMN_SIZE")
          if (columns.getString("IS_NULLABLE").toUpperCase.equals("NO"))
            notNullColumnsList = colName :: notNullColumnsList

          if (colTypeName.equalsIgnoreCase("CLOB") || colTypeName.contains(
                "TIMESTAMP") || colTypeName.equalsIgnoreCase("DATE") || colTypeName
                .equalsIgnoreCase("BLOB") || colTypeName.equalsIgnoreCase(
                "SYS.XMLTYPE")) {
            targetColumnMeta += (colName -> Utility.cleanDataType(colTypeName))
          } else if (colTypeName.equalsIgnoreCase("RAW")) {
            targetColumnMeta += (colName -> (colTypeName + "(" + colPrecision + ")"))
          } else if (colTypeName.equalsIgnoreCase("NUMBER") && colPrecision == "126" && colScale == "-127") {
            targetColumnMeta += (colName -> "NUMBER")
          } else if (colScale != null && colScale != "0") {
            targetColumnMeta += (colName -> (colTypeName + "(" + colPrecision + "," + colScale + ")"))
          } else {
            targetColumnMeta += (colName -> (colTypeName + "(" + colPrecision + ")"))
          }
        }
        if (targetColumnMeta.isEmpty) {
          throw new Exception(s"Error getting Schema for table $targetTable")
        }
        schemaResult = Option(targetColumnMeta)
        notNullableColumns = Option(notNullColumnsList.distinct)
      } catch {
        case exception: Exception =>
          log.info(s"Error getting Schema For Table $targetTable:" + exception)
          throw exception
      } finally {
        ConnectionFactory.closeConnection(connection.get)
      }
    }
    (schemaResult, notNullableColumns)
  }

  def mergeTempTableToTargetHUB(connection: Option[Connection],
                                tempTable: String,
                                targetTable: String,
                                hashKeyColumn: String,
                                businessKeyColumns: List[String]): Int = {
    var countMerged: Int = 0
    if (connection.isDefined) {
      log.info(s"Merging Temp Table $tempTable into Target Table $targetTable")
      try {
        val statement = connection.get.createStatement()
        countMerged = statement.executeUpdate(
          s"MERGE INTO $targetTable a USING (SELECT $hashKeyColumn, ${businessKeyColumns
            .mkString(",")}, $LOADDATE_COL, $AUDITID_COL, $RECORDSOURCE FROM $tempTable) b ON (a.$hashKeyColumn = b.$hashKeyColumn) WHEN NOT MATCHED THEN INSERT (a.$hashKeyColumn, ${businessKeyColumns
            .map(c => s"a.$c")
            .mkString(", ")}, a.$LOADDATE_COL, a.$AUDITID_COL, a.$RECORDSOURCE) VALUES (b.$hashKeyColumn, ${businessKeyColumns
            .map(c => s"b.$c")
            .mkString(", ")}, to_timestamp(b.$LOADDATE_COL, 'YYYY-MM-DD HH24:MI:SS.FF9'), b.$AUDITID_COL, b.$RECORDSOURCE)")
        statement.close()
        log.info(
          s"Merging Completed in RAW Vault with table $targetTable, no of records merged: $countMerged")
      } catch {
        case sqlException: SQLException =>
          log.error(
            s"Unexpected error! Merging Temporary table to Traget Table, " + sqlException)
          throw sqlException

      }
    }
    countMerged
  }

  def mergeTempTableToTargetLink(connection: Option[Connection],
                                 tempTable: String,
                                 targetTable: String,
                                 linkHashKeyName: String,
                                 linkKeys: String,
                                 link_keys_a: String,
                                 link_keys_b: String): Int = {
    var countMerged: Int = 0
    if (connection.isDefined) {
      log.info(
        s"Merging Temporal Table $tempTable into Target Table $targetTable")
    }
    try {
      val statement = connection.get.createStatement()
      countMerged = statement.executeUpdate(
        s"MERGE INTO $targetTable a USING (SELECT $linkHashKeyName, $linkKeys, $ERROR_INFO_COL, " + s"$LOADDATE_COL, $AUDITID_COL, $RECORDSOURCE FROM $tempTable) b " + s"ON (a.$linkHashKeyName = b.$linkHashKeyName) WHEN NOT MATCHED THEN" + s"INSERT (a.$linkHashKeyName, $link_keys_a, a.$ERROR_INFO_COL, a.$LOADDATE_COL, a.$AUDITID_COL," +
          s"a.$RECORDSOURCE) VALUES (b.$linkHashKeyName, $link_keys_b, b.$ERROR_INFO_COL, " + s"to_timestamp(b.$LOADDATE_COL, 'YYYY-MM-DD HH24:MI:SS:FF9'), b.$AUDITID_COL, b.$RECORDSOURCE)"
      )
      statement.close()
      log.info(
        s"Merging Completed in Raw Vault with table $targetTable, no of records merged: $countMerged")

    } catch {
      case sqlException: SQLException => {
        log.error(
          s"Unexpected error! Merging Temporary table to Target Table, " + sqlException)
        throw sqlException
      }
    }
    countMerged
  }

  def mergeTempTableToTargetSAT(connection: Option[Connection],
                                tempTable: String,
                                targetTable: String,
                                hashBusinessKey: String,
                                multiPartJoin: String,
                                satelliteKeys: String,
                                satelliteKeysA: String,
                                satelliteKeysB: String): Int = {
    var countMerged: Int = 0
    if (connection.isDefined) {
      log.info(
        s"Merging Temporal Table $tempTable into Target Table $targetTable")
      try {
        val statement = connection.get.createStatement()
        countMerged = statement.executeUpdate(
          s"MERGE INTO $targetTable a USING (SELECT $hashBusinessKey, $satelliteKeys, " + s"$VALIDFROM_TZ_COL, $VALIDFROM_TZ_COL as $VALIDFROM_COL, $POS_COL, $OPTYPE_COL, " +
            s"$ERROR_INFO_COL, $LOADDATE_COL, $AUDITID_COL, $RECORDSOURCE FROM $tempTable) b " +
            s"ON (a.$hashBusinessKey = b.$hashBusinessKey AND a.$VALIDFROM_COL = " + s"cast((TO_TIMESTAMP_TZ(b.$VALIDFROM_COL, '$UTC_TIMEFORMAT') at time zone 'uct') as timestamp)" + s"$multiPartJoin ) WHEN NOT MATCHED THEN INSERT (a.$hashBusinessKey, $satelliteKeysA, a.$VALIDFROM_TZ_COL, " +
            s"a.$VALIDFROM_COL, a.$POS_COL, a.$OPTYPE_COL, a.$ERROR_INFO_COL, a.$LOADDATE_COL, a.$AUDITID_COL," + s"a.$RECORDSOURCE) VALUES (b.$hashBusinessKey, $satelliteKeysB, " + s"to_timestamp_tz(b.$VALIDFROM_TZ_COL, '$UTC_TIMEFORMAT'), " + s"cast((TO_TIMESTAMP_TZ(b.$VALIDFROM_TZ_COL,'$UTC_TIMEFORMAT') at time zone 'uct' as timestmp)," +
            s" b.$POS_COL, b.$OPTYPE_COL, b.$ERROR_INFO_COL, to_timestamp(b.$LOADDATE_COL, 'YYYY-MM-DD HH24:MI:SS.FF9'), " +
            s"b.$AUDITID_COL,b.$RECORDSOURCE)")
        statement.close()
        log.info(
          s"Merging Completed in Raw Vault with table $targetTable, no of records merged: $countMerged")
      } catch {
        case sqlException: SQLException =>
          log.error(
            s"Unexpected error! Mering Temporary table to Target Table, " + sqlException)
          throw sqlException
      }
    }
    countMerged
  }

  def createTable(temporalTableName: String, columnList: String): Unit = {
    val connection =
      ConnectionFactory.getConnection(DBConnectionEnum.DATARAW_CONNECTION)
    if (connection.isDefined) {
      log.info("Creating temporal Table: " + temporalTableName)
      try {
        val statement = connection.get.createStatement()
        val tmpTableSql: String = s"" +
          s"DECLARE tblcount INTEGER; " +
          s"create_sql VARCHAR2(4000) := 'CREATE TABLE $temporalTableName ( $columnList )'; " +
          s"BEGIN SELECT COUNT(*) INTO tblcount FROM user_tables " +
          s"WHERE table_name = '${temporalTableName.toUpperCase()}'; " +
          s"IF tblCount = 0 THEN EXECUTE IMMEDIATE create_sql; END IF; END; "
        statement.executeQuery(tmpTableSql)
        connection.get.commit()
      } catch {
        case sqlException: SQLException =>
          log.error(
            s"Unexpected error! Creating Temporary table, " + sqlException)

          throw sqlException
      } finally {
        ConnectionFactory.closeConnection(connection.get)
      }
    }
  }

  def deleteTempTable(tempTableName: String): Unit = {
    val connection =
      ConnectionFactory.getConnection(DBConnectionEnum.DATARAW_CONNECTION)
    if (connection.isDefined) {
      log.info(s"Deleting temporal Table: " + tempTableName)
      try {
        val dropTmpTableStatement = connection.get.createStatement()
        val tmpTableSql = s"DECLARE tblcount INTEGER; " + s"drop_sql VARCHAR2(1000) := 'DROP TABLE $tempTableName'; BEGIN SELECT COUNT(*) INTO tblcount FROM user_tables WHERE table_name = '${tempTableName.toUpperCase}'; " +
          s"IF tblcount > 0 THEN EXECUTE IMMEDIATE drop_sql; END IF; END;"
        dropTmpTableStatement.executeQuery(tmpTableSql)
        dropTmpTableStatement.close()
      } catch {
        case exception: Exception =>
          log.error(s"Error deleting Temporary table: " + exception)
      } finally {
        ConnectionFactory.closeConnection(connection.get)
      }
    }
  }
}
