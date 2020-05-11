package com.test.framework.datavalidator

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.udf

import java.text.SimpleDateFormat
import java.util.Base64
import com.test.framework.common.ErrorInfoClass
import com.test.framework.common.Constants._
import com.test.framework.common.Utility._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.lit
import util.parsing.json.JSON._
import scala.util.Try

object DataValidator {

  private val log:Logger = Logger.getLogger(getClass.getName)

  private val convertDateFormatUDF = udf((inputDate:String,dateFormat:String)=> {

    try{
      val parsedDate = ZonedDateTime.parse(inputDate,DateTimeFormatter.ofPattern(dateFormat))
      val targetFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS XXX")
      targetFormat.format(parsedDate)
    }
    catch {
      case _:Exception => null
    }
  })

  private  val validDateFormatUDF = udf((inputDate:String, errorInfo:Seq[String],columnName:String,dateFormat:String,process:String,sourceTable:String)=> {
    try{
      if(inputDate != null){
        ZonedDateTime.parse(inputDate, DateTimeFormatter.ofPattern(dateFormat))
      }
    }catch{
      case _: Exception => errorInfo ++ Seq(gson.toJson(ErrorInfoClass(process,"Timestamp format validation", columnName, inputDate, "incorrect Timestmap format", sourceTable)))
    }
    errorInfo
  })

  private val validateCasting = udf((beforeCasting:String,afterCasting:String, errorInfo:Seq[String],columnName:String,expectedDataType:String,process:String,sourceTable:String) => {
    var result = errorInfo

    if(beforeCasting != null && afterCasting == null){
      result = errorInfo ++ Seq (gson.toJson(ErorInfoClass(process,"DataType Casting",columnName, beforeCasting, s"Casting failed to $expectedDataType",sourceTable)))
    }
    result
  })


  private val nullCheck = udf ((columnName:String) => {

    val dataJsonMap = parseFull(columnName).getOrElse(Map()).asInstanceOf[Map[String,Any]]
    val dataMap = flattenJson(dataJsonMap)
    val after = dataMap.fliter(c => c._1.contains("after_") && c._2 == null)
    after.keysIterator.toSeq
  })

  private val validateTypeSHA1 = udf ((dfCol:String,size:Int)=>{
    if(dfCol != null && dfCol.length >40)
      dfCol.substring(0,39)
    else
      dfCol
  })

  private val validateTypeBinary = udf((dfCol:Array[Byte],size)=>{
    if(dfCol !=null && dfCol.length >size)
      null
    else
      dfCol
  })

  private val validateTypeVarchar = udf((dfCol:String,size:Int)=>{
    if(dfCol!=null && dfCol.length>size)
      null
    else
      dfCol
  })

  private val validateIntervalds = udf((dfCol:String, dayPrecision:String,fractionSeconds:String) =>{
    val reg = s"(^[+-]?\\d{1,$dayPrecision} (?:\\d|[01]\\d|2[0-3]):[0-5]?[0-9]:[0-5]?[0-9](?:\\.\\d{1,$fractionSeconds})?$$"
    if(dfCol!=null && !dfCol.matches(reg))
      null
    else
      dfCol
  })

  private val validateTypeClob = udf((dfCol:Array[Byte])=>dfCol)

  private val validateTypeBlob = udf((dfCol:Array[Byte])=>dfCol)

  private val validateTypeXmlType = udf((dfCol:Array[Byte])=>dfCol)

  private val validateTypeTimestamp = udf((dfCol:String) => {
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

    if(dfCol != null &&
    !(Try(ZonedDateTime.parse(dfCol,formatter)).isSuccess ||
      Try(ZonedDateTime.parse(dfCol,formatter2)).isSuccess ||
      Try(ZonedDateTime.parse(dfCol,formatter3)).isSuccess ||
      Try(sdf1.parse(dfCol)).isSuccess ||
      Try(sdf2.parse(dfCol)).isSuccess ||
      Try(sdf3.parse(dfCol)).isSuccess
      )){
      null
    }else{
      dfCol
    }
  })

  private val validateTypeNumber = udf((dfCol:String,s1:String,s2:String,s3:String) => {
    if(dfCol!=null){
      if(s1==null && s2==null && s3==null && !((Try(dfCol.toDouble).isSuccess && dfCol.length <=39 && dfCol.contains(".") || (Try(dfCol.toDouble).isSuccess && dfCol.length <=38 && !dfCol.contains(".")))))
        null
      else if(s1!=null && s2==null && s3 == null && !(Try(dfCol.toDouble).isSuccess && dfCol.length <= s1.toInt && !dfCol.contains(".")))
        null
      else if(s2!=null && !((Try(dfCol.toDouble).isSuccess && dfCol.length <= s2.toInt +1 && dfCol.contains(".")) || (Try(dfCol.toDouble).isSuccess && dfCol.length <= s2.toInt && !dfCol.contains("."))))
        null
      else
        dfCol
    }else
      dfCol
  })

 private val validateTypeFloat = udf((dfCol:String) => {
   if(dfCol!=null && !((Try(dfCol.toDouble).isSuccess && dfCol.length <= 39 && dfCol.contains(".")) || (Try(dfCol.toDouble).isSuccess && dfCol.length <= 38 && !dfCol.contains("."))))
     {
       null
     }
   dfCol
 })

}
