package com.test.framework.datavalidator

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.udf
import java.text.SimpleDateFormat
import java.util.Base64
import com.test.framework.common.JSonParser
import com.test.framework.common.ErrorInfoClass
import com.test.framework.common.Constants._
import com.test.framework.common.Utility._
import jdk.nashorn.internal.parser.JSONParser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.lit

import util.parsing.json.JSON._
import scala.util.Try

object DataValidator {

  private val log: Logger = Logger.getLogger(getClass.getName)

  private val convertDateFormatUDF = udf(
    (inputDate: String, dateFormat: String) => {

      try {
        val parsedDate =
          ZonedDateTime.parse(inputDate,
                              DateTimeFormatter.ofPattern(dateFormat))
        val targetFormat =
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS XXX")
        targetFormat.format(parsedDate)
      } catch {
        case _: Exception => null
      }
    })

  private val validDateFormatUDF = udf(
    (inputDate: String,
     errorInfo: Seq[String],
     columnName: String,
     dateFormat: String,
     process: String,
     sourceTable: String) => {
      try {
        if (inputDate != null) {
          ZonedDateTime.parse(inputDate,
                              DateTimeFormatter.ofPattern(dateFormat))
        }
      } catch {
        case _: Exception =>
          errorInfo ++ Seq(
            gson.toJson(
              ErrorInfoClass(process,
                             "Timestamp format validation",
                             columnName,
                             inputDate,
                             "incorrect Timestmap format",
                             sourceTable)))
      }
      errorInfo
    })

  private val validateCasting = udf(
    (beforeCasting: String,
     afterCasting: String,
     errorInfo: Seq[String],
     columnName: String,
     expectedDataType: String,
     process: String,
     sourceTable: String) => {
      var result = errorInfo

      if (beforeCasting != null && afterCasting == null) {
        result = errorInfo ++ Seq(
          gson.toJson(
            ErrorInfoClass(process,
                          "DataType Casting",
                          columnName,
                          beforeCasting,
                          s"Casting failed to $expectedDataType",
                          sourceTable)))
      }
      result
    })

  private val nullCheck = udf((columnName: String) => {

    val dataJsonMap =
      parseFull(columnName).getOrElse(Map()).asInstanceOf[Map[String, Any]]
    val dataMap = flattenJson(dataJsonMap)
    val after = dataMap.filter(c => c._1.contains("after_") && c._2 == null)
    after.keysIterator.toSeq
  })

  private val validateTypeSHA1 = udf((dfCol: String, size: Int) => {
    if (dfCol != null && dfCol.length > 40)
      dfCol.substring(0, 39)
    else
      dfCol
  })

  private val validateTypeBinary = udf((dfCol: Array[Byte], size) => {
    if (dfCol != null && dfCol.length > size)
      null
    else
      dfCol
  })

  private val validateTypeVarchar = udf((dfCol: String, size: Int) => {
    if (dfCol != null && dfCol.length > size)
      null
    else
      dfCol
  })

  private val validateTypeIntervalds = udf(
    (dfCol: String, dayPrecision: String, fractionSeconds: String) => {
      val reg =
        s"(^[+-]?\\d{1,$dayPrecision} (?:\\d|[01]\\d|2[0-3]):[0-5]?[0-9]:[0-5]?[0-9](?:\\.\\d{1,$fractionSeconds})?$$"
      if (dfCol != null && !dfCol.matches(reg))
        null
      else
        dfCol
    })

  private val validateTypeClob = udf((dfCol: Array[Byte]) => dfCol)

  private val validateTypeBlob = udf((dfCol: Array[Byte]) => dfCol)

  private val validateTypeXmlType = udf((dfCol: Array[Byte]) => dfCol)

  private val validateTypeTimestamp = udf((dfCol: String) => {
    val dateFormat = "yyyy-MM-dd HH:mm:ss.SSS XXX"
    val dateFormat2 = "yyyy-MM-dd HH:mm:ss.SSSSSS XXX"
    val dateFormat3 = "yyyy-MM-dd HH:mm:ss.SSSSSSSSSS XXX"
    val dateFormat4 = "yyyy-MM-dd HH:mm:ss.SSS"
    val dateFormat5 = "yyyy-MM-dd HH:mm:ss.SSSSSS"
    val dateFormat6 = "yyyy-MM-dd HH:mm:ss.SSSSSSSSSS"

    val formatter = DateTimeFormatter.ofPattern(dateFormat)
    val formatter2 = DateTimeFormatter.ofPattern(dateFormat2)
    val formatter3 = DateTimeFormatter.ofPattern(dateFormat3)

    val sdf1 = new SimpleDateFormat(dateFormat4)
    val sdf2 = new SimpleDateFormat(dateFormat5)
    val sdf3 = new SimpleDateFormat(dateFormat6)

    if (dfCol != null &&
        !(Try(ZonedDateTime.parse(dfCol, formatter)).isSuccess ||
          Try(ZonedDateTime.parse(dfCol, formatter2)).isSuccess ||
          Try(ZonedDateTime.parse(dfCol, formatter3)).isSuccess ||
          Try(sdf1.parse(dfCol)).isSuccess ||
          Try(sdf2.parse(dfCol)).isSuccess ||
          Try(sdf3.parse(dfCol)).isSuccess)) {
      null
    } else {
      dfCol
    }
  })

  private val validateTypeNumber = udf((dfCol: String,
                                        s1: String,
                                        s2: String,
                                        s3: String) => {
    if (dfCol != null) {
      if (s1 == null && s2 == null && s3 == null && !((Try(dfCol.toDouble).isSuccess && dfCol.length <= 39 && dfCol
            .contains(".") || (Try(dfCol.toDouble).isSuccess && dfCol.length <= 38 && !dfCol
            .contains(".")))))
        null
      else if (s1 != null && s2 == null && s3 == null && !(Try(dfCol.toDouble).isSuccess && dfCol.length <= s1.toInt && !dfCol
                 .contains(".")))
        null
      else if (s2 != null && !((Try(dfCol.toDouble).isSuccess && dfCol.length <= s2.toInt + 1 && dfCol.contains(
                 ".")) || (Try(dfCol.toDouble).isSuccess && dfCol.length <= s2.toInt && !dfCol
                 .contains("."))))
        null
      else
        dfCol
    } else
      dfCol
  })

  private val validateTypeFloat = udf((dfCol: String) => {
    if (dfCol != null && !((Try(dfCol.toDouble).isSuccess && dfCol.length <= 39 && dfCol
          .contains(".")) || (Try(dfCol.toDouble).isSuccess && dfCol.length <= 38 && !dfCol
          .contains(".")))) {
      null
    }
    dfCol
  })

  private val validateTypeBinaryErrorInfo = udf(
    (dfCol: Array[Byte],
     sambaTypeDFCol: Array[Byte],
     colName: String,
     targetType: String,
     errorInfo: Seq[String],
     sourceTopic: String,
     process: String) => {

      if (dfCol == null && sambaTypeDFCol == null) {
        errorInfo
      } else if (dfCol == null && sambaTypeDFCol != null) {
        errorInfo ++ Seq(
          gson.toJson(
            ErrorInfoClass(process,
                           "Target datatype validation",
                           colName,
                           "null",
                           s"Incorrect type found instead of $targetType",
                           sourceTopic)))
      } else if (dfCol.deep == sambaTypeDFCol.deep) {

        errorInfo
      } else {
        errorInfo ++ Seq(
          gson.toJson(
            ErrorInfoClass(process,
                           "Target datatype validation",
                           colName,
                           Base64.getEncoder.encodeToString(dfCol),
                           s"Incorrect type found instead of $targetType",
                           sourceTopic)))
      }
    })

  private val validateTypeErrorInfo = udf((dfCol:String,sambaTypeDFCol:String,colName:String,targetType:String,errorInfo:Seq[String],sourceTopic:String,process:String)=>{
    if(dfCol == sambaTypeDFCol){
      errorInfo
    }else{
      errorInfo ++ Seq(
        gson.toJson(
          ErrorInfoClass(process,
            "Target datatype validation",
            colName,
            if (dfCol == null) "null" else dfCol,
            s"Incorrect type found instead of $targetType",
            sourceTopic)))
    }
  })

  def targetTypeValidation(inputDF:DataFrame, tableSchema:Map[String,String],process:String):DataFrame = {

    val dfColumns = inputDF.columns.toList diff List(ERROR_INFO_COL,SABA_TOPICNAME)
    val dfSchema = inputDF.schema.map(x => (x.name,x.dataType)).toMap
    var outputDF = inputDF

    for(dfCol <- dfColumns){
      val targetType = tableSchema.getOrElse(dfCol.toUpperCase, null)
      val castToType = dfSchema.getOrElse(dfCol, StringType)
      if(targetType ==null) {
        log.error(s"Column ${dfCol.toUpperCase} not found in oracle schema")
        throw new Exception(s"Column ${dfCol.toUpperCase} not found in oracle schema")
      }

      outputDF = targetType match {
        case typeCol if targetType.startsWith("RAW") && dfCol.endsWith("_HK") => {

          val matches = RAW_PATTERN.r.findAllMatchIn(typeCol).next()
          val size = matches.group(1).toInt
          outputDF=outputDF.withColumn("sabaType_"+dfCol, validateTypeSHA1(outputDF(dfCol),lit(size)))
          outputDF.withColumn(ERROR_INFO_COL,validateTypeErrorInfo(outputDF(dfCol),outputDF("sabaType_"+dfCol), lit(dfCol),lit(targetType),outputDF(ERROR_INFO_COL),outputDF(SABA_TOPICNAME),lit(process)))
        }
        case typeCol if targetType.startsWith("RAW") =>{
          val matches = RAW_PATTERN.r.findAllMatchIn(typeCol).next()
          val size = matches.group(1).toInt
          outputDF = outputDF.withColumn("sabaType_"+dfCol, validateTypeSHA1(outputDF(dfCol),lit(size)))
          outputDF.withColumn(ERROR_INFO_COL,validateTypeBinaryErrorInfo(outputDF(dfCol),outputDF("sabaType_"+dfCol), lit(dfCol),lit(targetType),outputDF(ERROR_INFO_COL),outputDF(SABA_TOPICNAME),lit(process)))
        }
        case _ if targetType.startsWith("FLOAT") => {
          outputDF.withColumn("sabaType_"+dfCol,validateTypeFloat(outputDF(dfCol)).cast(castToType))
        }
        case typeCol if targetType.startsWith("NUMBER") => {
          val matches = NUMBER_PATTERN.r.findAllMatchIn(typeCol)next()
          outputDF.withColumn(ERROR_INFO_COL,validateTypeNumber(outputDF(dfCol),outputDF("sabaType_"+dfCol), lit(dfCol),lit(targetType),outputDF(ERROR_INFO_COL),outputDF(SABA_TOPICNAME),lit(process)))
        }
        case typeCol if targetType.startsWith("INTERVALDS") => {
          val matches = INTERVALDS_PATTERN.r.findAllMatchIn(typeCol).next()
          outputDF.withColumn(ERROR_INFO_COL,validateTypeIntervalds(outputDF(dfCol),outputDF("sabaType_"+dfCol), lit(dfCol),lit(targetType),outputDF(ERROR_INFO_COL),outputDF(SABA_TOPICNAME),lit(process)))
        }
        case _ if targetType.startsWith("TIMESTAP") => {
          outputDF.withColumn("sabaType_"+dfCol,validateTypeTimestamp(outputDF(dfCol)))
        }
        case typeCol if targetType.startsWith("VARCHAR") => {
          val matches = VARCHAR_PATTERN.r.findAllMatchIn(typeCol).next()
          outputDF.withColumn("sabaType_"+dfCol,validateTypeVarchar(outputDF(dfCol),lit(matches.group(1).toInt)))
        }
        case _ if targetType.equalsIgnoreCase("CLOB") => {
          outputDF.withColumn("sabaType_"+dfCol,validateTypeClob(outputDF(dfCol)))
        }
        case _ if targetType.equalsIgnoreCase("BLOB") => {
          outputDF.withColumn("sabaType_"+dfCol,validateTypeBlob(outputDF(dfCol)))
        }
        case _ if targetType.equalsIgnoreCase("SYS.XMLTYPE") => {
          outputDF.withColumn("sabaType_"+dfCol,validateTypeXmlType(outputDF(dfCol)))
        }
        case _ =>{
          log.error(s"Target side validation is not yet defined for $targetType, need to be implemented. Proceeding without validation")
        outputDF.withColumn("sabaType_"+dfCol,outputDF(dfCol))
        }

      }

      if(!targetType.startsWith("RAW")){
        outputDF = outputDF.withColumn(ERROR_INFO_COL, validateTypeErrorInfo(outputDF(dfCol),outputDF("sabaType_"+dfCol),lit(dfCol),lit(targetType),outputDF(ERROR_INFO_COL),outputDF(SABA_TOPICNAME),lit(process)))
      }
    }

    outputDF.printSchema()

    val colList = outputDF.columns.toList.filter(x => !x.startsWith("sabaType_") && x != ERROR_INFO_COL && x!=SABA_NULL_COL)
    val resultDF= outputDF.drop(colList: _*)

    resultDF.columns.foldLeft(resultDF) {(newDF,colName) => newDF.withColumnRenamed(colName,colName.replace("sabaType_",""))}

  }

  def schemaCheck(dataDF:DataFrame, sourceSchema:Map[String,Any], sourceTable:String, process:String):DataFrame = {
    val jsonToSchema = udf((s:String, errorInfo:Seq[String]) => {
      val dataJsonMap = JSonParser.parse(s).getOrElse(Map()).asInstanceOf[Map[String,Any]]
      val dataMap = flattenJson(dataJsonMap)
      val dataKeyList = dataMap.keysIterator.toList

      val diff = dataKeyList.diff(sourceSchema.keysIterator.toList)
      if(diff.nonEmpty){
        var message:List[String] = List.empty
        diff.foreach{colName =>
          message = message :+ gson.toJson(ErrorInfoClass(process,"schema validation",colName, dataMap.getOrElse(colName, "null").toString,"Schema Validation Failed", sourceTable))
        }
        errorInfo ++ Seq(message.mkString(","))
      }
      else
        errorInfo
    })

    val dfWithMissingCols = dataDF.withColumn(ERROR_INFO_COL, jsonToSchema(dataDF(SABA_VALUE_COL),dataDF(ERROR_INFO_COL)))
    dfWithMissingCols
  }

  def sabaNullCheck(df:DataFrame): DataFrame ={
    val dfNullCheckCols = df.withColumn(SABA_NULL_COL, nullCheck(df(SABA_VALUE_COL)))
    dfNullCheckCols
  }

  def validateCasting(df:DataFrame, flattenSchemaJsonMap:Map[String, String],sourceColList:List[String],headerList:List[String],sourceTable:String, process:String):DataFrame = {
    var rdf = df

    sourceColList.foreach{x =>
      rdf = rdf.withColumn(ERROR_INFO_COL, validateCasting(rdf(s"before_$x"),rdf(s"afterCasting_before_$x"),rdf(ERROR_INFO_COL),lit(x),lit(flattenSchemaJsonMap(s"before_${x.toLowerCase}")),lit(process),lit(sourceTable)))

      rdf = rdf.withColumn(ERROR_INFO_COL, validateCasting(rdf(s"after_$x"),rdf(s"afterCasting_after_$x"),rdf(ERROR_INFO_COL),lit(x),lit(flattenSchemaJsonMap(s"after_${x.toLowerCase}")),lit(process),lit(sourceTable)))
    }

    headerList.foreach{x =>
      rdf = rdf.withColumn(ERROR_INFO_COL, validateCasting(rdf(x),rdf(s"afterCasting_$x"),rdf(ERROR_INFO_COL),lit(x),lit(flattenSchemaJsonMap(s"${x.toLowerCase}")),lit(process),lit(sourceTable)))
    }

    val colList = rdf.columns.toList.filter(x => !x.startsWith("afterCasting_") && x != SABA_NULL_COL && x!= INV_RECORD_MESSAGE_COL)

    val rdf2 = rdf.drop(colList:_*)

    val resultDF = rdf2.columns.foldLeft(rdf2){(newDF, colName) => newDF.withColumnRenamed(colName, colName.replace("afterCasting_",""))}
    resultDF
  }

  def validateTimeFormat(dataDF:DataFrame, flattenSchemaJsonMap: Map[String,Any],requiredColList:List[String],sourceTable:String, process:String):DataFrame ={
    val timestampColMap = flattenSchemaJsonMap.filter(x => x._2.toString.contains("timestamp")).asInstanceOf[Map[String,String]]
    val timeStampColsList = timestampColMap.keys.toList.intersect(requiredColList.distinct)

    val validatedTimeStampDF = timeStampColsList.foldLeft(dataDF){(newDF, colName) =>
      newDF.withColumn(ERROR_INFO_COL,validDateFormatUDF(newDF(colName),newDF(ERROR_INFO_COL),lit(colName),lit(timestampColMap(colName).replaceAll("timestamp ",""),lit(process),lit(sourceTable))))}

    timeStampColsList.foldLeft(validatedTimeStampDF){(newDF, colName) =>
      newDF.withColumn(colName,convertDateFormatUDF(newDF(colName),lit(timestampColMap(colName).replaceAll("timestamp ",""))))}

  }

}
