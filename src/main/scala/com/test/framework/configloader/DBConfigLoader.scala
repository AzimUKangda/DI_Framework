package com.test.framework.configloader
import com.test.framework.config.{GenericConfig, ListGenericConfig}
import org.apache.log4j.Logger
class DBConfigLoader(source: String,
                     filterColumn: List[String],
                     var queryExecutor: DBQueryExecutor = new DBQueryExecutor)
    extends ConfigLoader(source, filterColumn) {

  var connEnum: DBConnectionEnum.Value = DBConnectionEnum.METADATA_CONNECTION
  val log: Logger = Logger.getLogger(getClass.getName)

  /**
    * This method creates the dinamic query to retrieve the configuration in the DB,based on the classToCast type,
    * but because it could query N results from query it returns a ListGenericConfig which is also a GenericConfig object
    *
    * If the filter parameter is specified it gets the mapping columns from the source parameters for example:
    * 1) case without filter
    * param: source="tableA", filter=None, classToCast:GenericConfig
    * result: SELECT .... from tableA
    *
    * 2) case with filter
    * param: source="table->tableA|filter->someColumn,someColumn2",filter=Some("valueCol,valueCol2"), classToCast:GenericConfig
    * result: select .... from tableA where someColumn = valueCol AND someColumn2 - valueCol2
    */
  override def loadConfig[A <: GenericConfig](filter: Option[List[String]],
                                              classToCast: A): GenericConfig = {
    var columnList = List[String]()
    var filterList = List[String]()
    val listResult = new ListGenericConfig()
    val classDefinition = classToCast.getClass

    log.info(s"Getting configuration for $source")
    classDefinition.getDeclaredFields.foreach(f => {
      columnList = f.getName.toUpperCase :: columnList
    })

    try {
      if (filter.isDefined) {
        for (i <- filterColumns.indices) {
          filterList = s"${filterColumns(i)} = '${filter.get(i)}'" :: filterList
        }
      }

      val (columnsResult, resultSet) = queryExecutor.executeQuery(
        queryExecutor.createDynamicQuery(columnList, source, filterList),
        connEnum)
      if (columnsResult.isDefined && resultSet.isDefined) {
        val columnNames = columnsResult.get
        for (row <- resultSet.get) {
          val newRow = classDefinition.newInstance()
          for (i <- columnNames.indices) {
            classDefinition.getMethods
              .find(_.getName.toUpperCase() == columnNames(i) + "_$EQ")
              .get
              .invoke(newRow, row(i))
          }
          listResult.listProperties = newRow :: listResult.listProperties
        }
      }
    } catch {
      case exception: IndexOutOfBoundsException =>
        log.error(
          "Error: The number of condition for the query doesn't match with the number of filter provided")
        throw exception
    }
    listResult
  }

}
