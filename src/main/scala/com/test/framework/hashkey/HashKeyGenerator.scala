package com.test.framework.hashkey

import java.time.{ZoneOffset,ZonedDateTime}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.{Column,DataFrame}
import org.apache.spark.sql.functions._
import com.test.framework.common.Constants._
import org.apache.log4j.Logger
import com.test.framework.common.JSonParser

object HashKeyGenerator {

  val log = Logger.getLogger(getClass.getName)

  val castBinaryToString = udf((p:Array[Byte]) => p.map(_.toChar).mkString)
  val castTimestampToString = udf((p:String) => {
    val parswedDate = ZonedDateTime.parse(p,DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS XXX"))
    val targetFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS XXX").withZone(ZoneOffset.UTC)
    targetFormat.format(parswedDate).replace(" Z","")
  })

  private def formatStringType(column:Column, colType:String):Column = {
    colType match {
      case "INT" => format_string("%020d.000000000000000000",column)
      case "FLOAT" => format_string("%039.18f",column)
      case typ if typ.startsWith("TIMESTAMP") => castTimestampToString(column)
      case "BINARY" => castBinaryToString(column)
      case _ => column
    }
  }


  private def generateHashKey(masterSchema:String,columns:Column*):Column ={
    val schemaMap = getSchemaDataTypeMap(masterSchema)
    sha1(encode(concat_ws(HASH_KEY_DELIMITER,columns.map(column => formatStringType(column,schemaMap(column.expr.verboseString.toUpperCase).toUpperCase)):_*),"UTF-16E"))
  }

  def getHashKeys(inputDF:DataFrame,listColumns:List[String],outputColumnName:String,masterSchema:String):DataFrame = {
    log.info("Genrating hashkey for columns:"+listColumns.mkString(","))
    val selectionColumns = listColumns.map(c=>col(c))
    inputDF.withColumn(outputColumnName,generateHashKey(masterSchema,selectionColumns:_*))
  }

  private def getSchemaDataTypeMap(masterSchema: String):Map[String,String] ={
    val schemaJson = JSonParser.parse(masterSchema)
    val schema_json_map = schemaJson.getOrElse(Map()).asInstanceOf[Map[String,Any]]

    val headerColMap = (schema_json_map -- List("after","before")).asInstanceOf[Map[String,String]]
    val beforeColMap = schema_json_map("before").asInstanceOf[Map[String,Any]].asInstanceOf[Map[String,String]]
    val tempMap = headerColMap ++ beforeColMap
    tempMap.map(x=>"'"+x._1.toUpperCase -> x._2)
  }

  def hubKeysWithAlias(withAlias:Boolean, alias:String, hubKeys:List[String],schema:Map[String,String]):String = {
    withAlias match {
      case _ if withAlias && alias.equals("b") => hubKeys.sortWith((x,y) => x>y).map(x=> if(schema.getOrElse(x.toUpperCase, "").contains("TIMESTAMP"))"to_timstamp_tz("+alias +"."+x+s",'$UTC_TIMEFORMAT')" else  alias + "."+x).reduce((x,y)=>x+","+y)
      case _ if withAlias => hubKeys.sortWith((x,y)=> x>y).map(x=>alias+"."+x).reduce((x,y)=>x+","+y)
      case _ => hubKeys.sortWith((x,y)=> x>y).map(x=>x).reduce((x,y)=>x+","+y)
    }
  }


  def getIndividaulHashKey(inputDF:DataFrame,hashKeyOrder:Map[String,List[String]],masterSchema:String):DataFrame = {
    var resultDF = inputDF
    hashKeyOrder.foreach{hubColumn => resultDF = getHashKeys(resultDF,hubColumn._2,hubColumn._1,masterSchema)}
    resultDF
  }

}
