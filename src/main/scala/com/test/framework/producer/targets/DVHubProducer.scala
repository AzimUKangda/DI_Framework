package com.test.framework.producer.targets

import java.sql.Connection

import com.test.framework.common.Utility
import com.test.framework.config.{GenericConfig, ListGenericConfig, MetadataHubConfig}
import com.test.framework.configloader.ConfigLoaderFactory
import com.test.framework.enums.{ConfigEnum, MetadataEnum}
import com.test.framework.producer.DVProducer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.test.framework.common.Constants._
import com.test.framework.hashkey.HashKeyGenerator

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

  override def createDVTragetDF(inputDF: DataFrame, masterSchema: String, currentSource: String, sourceColumn: List[String], sourceColumnWithOrder: List[String], targetHKColumn: String, businessKeyColumn: List[String]): DataFrame = {
    val loadDate:String = Utility.currentTimeInMilliUTC

    var filteredColumns = targetHKColumn :: sourceColumnWithOrder
    filteredColumns = s"$ERROR_INFO_COL" ::filteredColumns
    filteredColumns = RECORDSOURCE :: filteredColumns

    val selectionColumns = filteredColumns.map(c => col(c))
    val hashKey = HashKeyGenerator.getHashKeys(inputDF,sourceColumnWithOrder,targetHKColumn,masterSchema)
    val filteredColsDF = hashKey.select(selectionColumns:_*)
    val combinedHubTmpDF = Utility.renameSrcDFtoTargetDF(filteredColsDF, (sourceColumn zip businessKeyColumn).toMap)
    combinedHubTmpDF.withColumn(LOADDATE_COL,lit(loadDate)).withColumn(AUDITID_COL,lit(auditId))

  }

  @throws(classOf[Exception])
  override def getMetadataValue(currentSource: Option[String], metadataValue: MetadataEnum.Value): List[String] = {
    var recordsSource:List[GenericConfig] = List()
    if(currentSource.isDefined)
      recordsSource = metadataMap.getOrElse(currentSource.get,null)
    else
      recordsSource = metadataMap.head._2

   var resultMetadata: List[String] = List()
    if(recordsSource != null) {
      val recordsSourceHub = recordsSource.asInstanceOf[List[MetadataHubConfig]]
      resultMetadata = metadataValue match {
        case MetadataEnum.SOURCE_COLUMN => recordsSourceHub.map(record => record.sourceColumn.toLowerCase).distinct
        case MetadataEnum.HASHKEY_COLUMN_ORDER => val sortedColumns = SortedMap[Int,String]()++ recordsSourceHub.map(
          record =>record.hubHashkeyOrder.toInt -> record.sourceColumn.toLowerCase
        ).toMap
          sortedColumns.values.toList
        case MetadataEnum.HASHKEY_COLUMN => recordsSourceHub.map(record => record.hubSkColumn)
        case MetadataEnum.BUSINESSKEY_COLUMN => recordsSourceHub.map(record => record.hubBusinessKeyColumn)
        case _ => throw  new Exception(s"It was not possible to find ${metadataValue.toString} metadata from source : $currentSource")
      }
    }else {
      throw new Exception(s"No metadata found for source $currentSource")
    }
    resultMetadata
  }

  override def dedupDF(df: DataFrame) :DataFrame = {
    log.info("deduplicating tables")
    val hashKey = getMetadataValue(None,MetadataEnum.HASHKEY_COLUMN).head
    df.dropDuplicates(hashKey)
  }

  override def writeDataToTarget(connection: Option[Connection]): Int = {
    val businesskeyColumns = getMetadataValue(None,MetadataEnum.BUSINESSKEY_COLUMN).distinct ::: List(ERROR_INFO_COL)
    val hashKeyColumn = getMetadataValue(None,MetadataEnum.HASHKEY_COLUMN).head
    myDBExecutor.mergeTempTableToTargetHUB(connection,temporaryTableName,targetTable,hashKeyColumn,businesskeyColumns)
  }

  override def coaleaseBusinessKey(inputDF: DataFrame, currentSource: String): DataFrame = {
    val sourceColumn = getMetadataValue(Option(currentSource),MetadataEnum.SOURCE_COLUMN)
    val commonColumns = sourceColumn.intersect(HEADER_LIST)
    val cleanedListColumns = sourceColumn.diff(HEADER_LIST)
    val selectEx = cleanedListColumns.map(
      c=> s"CASE WHEN array_contains($SABA_NULL_COL, 'after_$c') then null else COALESCE(after_$c, before_$c) end as $c"
    )

    if(commonColumns.isEmpty)
      inputDF.selectExpr(selectEx ::: List(ERROR_INFO_COL,RECORDSOURCE,INV_RECORD_MESSAGE_COL):_*)
    else
      inputDF.selectExpr(selectEx ::: List(ERROR_INFO_COL,RECORDSOURCE,INV_RECORD_MESSAGE_COL) ++ commonColumns:_*)
  }

// For hub we simply delete invalid records without writing it into trash table
  override def errorHandling(inputDF: DataFrame, currentSource: String): DataFrame = {
    val colsTovalidate = notNullableColumns ::: getMetadataValue(Option(currentSource),MetadataEnum.HASHKEY_COLUMN_ORDER)
    val invalidRecordsDF = Utility.getInvalidNullRecords(inputDF, colsTovalidate)
    inputDF.except(invalidRecordsDF).drop(SABA_NULL_COL)
  }

}
