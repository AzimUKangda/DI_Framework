package com.test.framework.common
import org.apache.log4j.Logger
object ApplicationLog {

  val log: Logger = Logger.getLogger(getClass.getName)
  val applicationID: String = context.spark.sparkContext.applicationId
  val applicationName: String = context.spark.sparkContext.appName
  val sparkUser: String = context.spark.sparkContext.sparkUser
  val trackingUrl: String = context.spark.conf
    .get(
      "spark.org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter.param.PROXY_URI_BASE",
      "Local Mode")
    .split(",")(0)
  var queryExecuter: DBQueryExecuter = new DBQueryExecuter

  def insertApplicationLog(pipelineName: String, layer: String): Unit = {
    try {
      val currentTime = Utility.getCurrentTime
      val insertValues = List[String](
        s"'$pipelineName'",
        s"'$applicationID'",
        s"'$layer.$applicationName'",
        s"'$trackingUrl'",
        s"'$sparkUser'",
        s"'$currentTime'",
        "' '",
        "'RUNNING'",
        "' '",
        s"'$layer'",
        "' '"
      )
      val insertQuery = queryExecuter.createDynamicInsert(SPARK_APPLICATION_LOG_TABLE, insertValues)
      queryExecuter.executeUpdate(insertQuery)
    }catch {
      case exception: Exception => log.error("Error inserting application log:" + exception)
    }
  }

  def updateApplicationLog(status: String, inputCount: String = null, insertCount: String= null): Unit = {
    try{
      val currentTime = Utility.getCurrentTime
      val filtersList = List[String](s"YARN_APPLICATION_ID='$applicationID'")
      val columnValues = List[String](s"ENDTIME = '$currentTime'", s"STATUS = '$status'",s"INPUT_COUNT = '$inputCount'", s"INSERT_COUNT = '$insertCount'")
      val updateQuery = queryExecuter.createDynamicUpdate(SPARK_APPLICATION_LOG_TABLE, columnValues, filtersList)
      queryExecuter.executeUpdate(updateQuery)
    }catch {
      case exception: Exception => log.error("Error updating application log:" + exception)
    }
  }

}
