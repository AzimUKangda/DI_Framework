package com.test.framework.producer

import org.apache.log4j.Logger
import java.util.UUID.randomUUID
import java.sql.Connection

import com.test.framework.common.{ApplicationContextProperties, ApplicationLog, JSonParser, Utility}
import com.test.framework.config.{DatabaseConfig, GenericConfig, ListGenericConfig, SourceSchemaConfig}
import com.test.framework.db.DBQueryExecutor
import com.test.framework.enums.{ConfigEnum, MetadataEnum}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.test.framework.common.Constants._
import com.test.framework.configloader.ConfigLoaderFactory
import com.test.framework.consumer.Consumer
import com.test.framework.datavalidator.DataValidator
import io.netty.handler.codec.http2.Http2HeadersEncoder.Configuration
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

abstract class DVProducer(val target:String, val myDBExecutor: DBQueryExecutor = new DBQueryExecutor)(implicit spark:SparkSession)extends Producer {

  val log = Logger.getLogger(getClass.getName)

  protected val auditId = randomUUID.toString
  protected val temporaryTableName = "tmp_"+auditId.substring(0,11).replaceAll("-","_")+"_"+System.currentTimeMillis().toString

  protected var tableSchema :Map[String,String] = Map()
  protected var notNullableColumns :List[String] = List()
  protected var metadataMap:Map[String,List[GenericConfig]] = Map()
  protected var sourceSchema : List[SourceSchemaConfig] = List()
  protected var dbProperties: DatabaseConfig =_
  protected var targetTable :String = target

  @throws(classOf[Exception])
  def  getMetadataValue(currentSource:Option[String],metadataValue:MetadataEnum.Value):List[String]

  def createDVTragetDF(inputDF:DataFrame,masterSchema:String,currentSource:String,sourceColumn:List[String],sourceColumnWithOrder:List[String],targetHKColumn:String,businessKeyColumn:List[String]):DataFrame

  def dedupDF(df:DataFrame) : DataFrame

  def coaleaseBusinessKey(inputDF:DataFrame,currentSource:String):DataFrame

  def errorHandling(inputDF:DataFrame,currentSource:String):DataFrame

  def createTmptable():Unit ={
    val columnList = tableSchema.map(
      x => x._1+" "+(
        if (x._2.contains("TIMESTAMP") || x._2.contains("INTERVALDS")) "varchar2(255)"
        else if (x._2.contains("XMLTYPE")) "XMLTYPE"
        else x._2
      )
    ).reduce(_ + " , "+ _)
    myDBExecutor.createTable(temporaryTableName,columnList)
  }

  def writeDFToTempTable(df:DataFrame):Unit={
    try{
      df.write
        .mode("append")
        .format("jdbc")
        .option("driver",dbProperties.driverDbRawdata)
        .option("url",dbProperties.urlDbRawdata)
        .option("dbtable",temporaryTableName)
        .option("user",dbProperties.userNameDbRawdata)
        .option("password",dbProperties.passwordDbRawdata)
        .option("numPartition",ApplicationContextProperties.maxNumberDBConnections)
        .option("batchSize",ApplicationContextProperties.batchSize)
        .save()

      log.info("Dataframe written to temp   table")

    }catch {
      case ex:Exception => {
        log.info("Error while writing to Tmp" + ex.getMessage)
        throw ex
      }
    }
  }


  def writeTOThrashTable(df:DataFrame,currentSource:String): Unit = {
    val loadDate = Utility.currentTimeInMilliUTC
    val filteredDF = df.select(INV_RECORD_MESSAGE_COL)
    val recordsToWriteDF = filteredDF.withColumn(INV_RECORD_TABLE_COL,lit(targetTable))
      .withColumn(INV_RECORD_TOPIC_COL,lit(currentSource))
      .withColumn(INV_RECORD_DATE_COL,to_timestamp(lit(loadDate)))
      .withColumn(INV_RECORD_ERROR_COL,lit(ERROR_MSG_INV_RECORD))
    try
    {
      recordsToWriteDF.write
        .mode("append")
        .format("jdbc")
        .option("driver",dbProperties.driverDbRawdata)
        .option("url",dbProperties.urlDbRawdata)
        .option("dbtable",temporaryTableName)
        .option("user",dbProperties.userNameDbRawdata)
        .option("password",dbProperties.passwordDbRawdata)
        .option("numPartition",ApplicationContextProperties.maxNumberDBConnections)
        .option("batchSize",ApplicationContextProperties.batchSize)
        .save()

      log.info("Invalid records written in Trash table")
    }catch
      {
        case ex:Exception => {
          log.info("Error writing to Trash table"+ex.getMessage)
          throw ex
        }
      }
  }

  def applyValidationsDF(inputDF:DataFrame,schema:String,currentSource:String,sourceColumn:List[String]):DataFrame={
    log.info(s"appplying validation to df for source $currentSource")
    val flattenSchemaJsonMap = Utility.flattenJson(JSonParser.parse(schema).getOrElse(Map()).asInstanceOf[Map[String,Any]])

    val schemaChkDF = DataValidator.schemaCheck(inputDF.withColumn(ERROR_INFO_COL,array()),flattenSchemaJsonMap,currentSource,ApplicationContextProperties.process)

    val nullChkDF = DataValidator.sabaNullCheck(schemaChkDF)
    val flattenedDF = Utility.flattenDFBatchJob(nullChkDF,currentSource,schema)
    val renamedDataDF = Utility.renameDFColumns(flattenedDF)

    val jsonSchemaLowerCase = flattenSchemaJsonMap.asInstanceOf[Map[String,String]]
      .map(x=>x._1.toLowerCase -> x._2)

    val castedDF = Utility.createCastedColumn(renamedDataDF,jsonSchemaLowerCase)

    val validatedDF = DataValidator.validateCasting(castedDF,jsonSchemaLowerCase,sourceColumn.diff(HEADER_LIST),HEADER_LIST,currentSource,ApplicationContextProperties.process)

    val validatedDateFormatDF = DataValidator.validateTimeFormat(validatedDF,flattenSchemaJsonMap,sourceColumn++HEADER_LIST,currentSource,ApplicationContextProperties.process)

    val rawHubDF = validatedDateFormatDF.withColumn(RECORDSOURCE,concat_ws("#",col("service_name"),col("schema"),col("table")))
rawHubDF
  }

  override def loadDataFromSources(consumer:Consumer): Unit = {
    var connectionTargetTable :Option[Connection] = None
    try{
      connectionTargetTable= myDBExecutor.lockTable(targetTable)
      if(connectionTargetTable.isDefined){
        ApplicationLog.insertApplicationLog(auditId,APPLICATION_LAND2RAWV)
        createTmptable()

        var resultDFandCount : Map[DataFrame,Long] = Map()
        var sourceOffsetMap = Map.empty[String,String]
        metadataMap.foreach{source =>
          val currentSource = source._1
          log.info("reading data from currentSource")
          val offsetTopic = consumer.readOffset(currentSource)
          if(offsetTopic.isDefined){
            var countRecords = 0L

            val sourceDF = consumer.readDataFromSource(currentSource,offsetTopic.get._1,offsetTopic.get._2).repartition(ApplicationContextProperties.rddRepartitionSize)
            val persistedDF = sourceDF.persist(StorageLevel.MEMORY_AND_DISK)
            countRecords = persistedDF.count()

            if(!offsetTopic.get._1.equals(offsetTopic.get._2)&&countRecords>0){
              var targetrenamedDF = spark.emptyDataFrame
              val addOriginalMsgDF = persistedDF.withColumn(INV_RECORD_MESSAGE_COL,col(SABA_VALUE_COL))

              val sourceSchemaList = sourceSchema.filter(_.pipelineObject == currentSource)
              if(sourceSchemaList.nonEmpty){
                val sourceColumn = getMetadataValue(Option(currentSource),MetadataEnum.SOURCE_COLUMN)
                val hashKeyColumnOrdered = getMetadataValue(Option(currentSource),MetadataEnum.HASHKEY_COLUMN_ORDER)
                val targetHKColumn = getMetadataValue(Option(currentSource),MetadataEnum.HASHKEY_COLUMN).head
                val businessKeyColumn = getMetadataValue(Option(currentSource),MetadataEnum.BUSINESSKEY_COLUMN)

                val validatedDF = applyValidationsDF(addOriginalMsgDF,sourceSchemaList.head.jsonSchema,currentSource,sourceColumn)
                val rawIncrementDF = coaleaseBusinessKey(validatedDF,currentSource)

                val clearDataDF = errorHandling(rawIncrementDF,currentSource)
                countRecords= clearDataDF.count()

                targetrenamedDF = createDVTragetDF(clearDataDF,sourceSchemaList.head.jsonSchema,currentSource,sourceColumn,hashKeyColumnOrdered,targetHKColumn,businessKeyColumn)

                val commitOffset = offsetTopic.get._2

                sourceOffsetMap = sourceOffsetMap ++ Map(currentSource -> commitOffset)

              }else{
                throw new Exception(s"No schema found for the source $currentSource")
              }
              resultDFandCount = resultDFandCount + (targetrenamedDF.withColumn(SABA_TOPICNAME,lit(currentSource)) -> countRecords)

            }else{
              log.info(s"No new Data available lo load, hence skipping the source $currentSource")
            }
          }
        }

        val totalInput = resultDFandCount.values.sum
        val sourceDFList = resultDFandCount.keys.filter(_!=null)
        if(totalInput > 0){
          val combineDF = sourceDFList.reduceLeft(_ unionByName _)
          val deduplicatedHubDF = dedupDF(combineDF)

          val afterTargetValidation = DataValidator.targetTypeValidation(deduplicatedHubDF,tableSchema,ApplicationContextProperties.process)
          val deltaDFToDB = afterTargetValidation.withColumn(ERROR_INFO_COL,concat(lit("["),concat_ws(",",col(ERROR_INFO_COL)),lit("]"))).drop(SABA_TOPICNAME)

          writeDFToTempTable(deltaDFToDB)

          val mergeCount = writeDataToTarget(connectionTargetTable)

          sourceOffsetMap.foreach{
             x => consumer.writeOffset(x._1,x._2,auditId)
          }
          connectionTargetTable.get.commit()
          ApplicationLog.updateApplicationLog(STATUS_SUCCESS,totalInput.toString,mergeCount.toString)

        }else{
          log.info(s"No new data to tload for table $targetTable")
          ApplicationLog.updateApplicationLog(STATUS_SUCCESS,totalInput.toString, "0")
        }
      }
    }finally {
      myDBExecutor.deleteTempTable(temporaryTableName)
      if(connectionTargetTable.isDefined){
        ConnectionFactory.closeConnection(connectionTargetTable.get)
      }
    }
  }

  @throws(classOf[Exception])
  override def loadConfig(): Unit = {

    log.info("Start loading base Metadata for table" + targetTable)
    val (tableSchemaOption,notNullableColumnsOption) = myDBExecutor.getTableSchema(targetTable)

    val configOption = ConfigLoaderFactory.getConfig(ConfigEnum.SOURCE_SCHEMA_CONFIG,Option(List(targetTable,"yes")))
    val dbPropertiesOption = ConfigLoaderFactory.getConfig(ConfigEnum.DATABSE_CONFIG)

    if(configOption.isDefined && tableSchemaOption.isDefined && dbPropertiesOption.isDefined && notNullableColumnsOption.isDefined){
      sourceSchema = configOption.get.asInstanceOf[ListGenericConfig].listProperties.asInstanceOf[List[SourceSchemaConfig]]
      tableSchema=tableSchemaOption.get
      notNullableColumns=notNullableColumnsOption.get
      dbProperties=dbPropertiesOption.get.asInstanceOf[DatabaseConfig]
    }
    else
      throw new Exception("Error we could not load Metadata from DVProducer")
  }

}
