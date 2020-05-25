package com.test.framework.common

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import com.google.gson.Gson
import com.test.framework.common.Constants.SABA_VALUE_COL
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}
import scala.reflect.ClassTag

case class ErrorInfoClass(process:String,
                          process_step:String,
                          attribute:String,
                          value:String,
                          error:String,
                          source:String)

object Utility {

  val gson = new Gson()

  /**
    * This method convert a string from camel case to point case
    * ex: someStringCamel => some.string.camel
    * @param name
    * @return
    */
  def camelToPointCase(name: String) =
    "[A-Z\\d]".r.replaceAllIn(name, { m =>
      "." + m.group(0).toLowerCase()
    })

  /**
    * This method converts a string from UnderScore Case to Camel Case
    * ex: Some_string_score => someStringScore
    * @param name
    * @return
    */
  def underscoreToCamel(name: String) =
    "_([a-z\\d])".r.replaceAllIn(name, { m =>
      m.group(1).toUpperCase()
    })

  /**
    * Utility method to clean a DB DataType, removing the size value
    * ex: VARCHAR(100) => VARCHAR
    */
  def cleanDataType(name: String): String = name.split("\\(")(0)

  def getCurrentTime: String =
    new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date)

  def flattenDFBatchJob(rawDF: DataFrame,
                        topic: String,
                        schema: String): DataFrame = {
    val dataDF =
      rawDF.filter(column("topic") === topic && column("value") =!= "")

    val schemaST: StructType = Context.spark.read.json(Seq(schema).toDS).schema
    val rdf1 = dataDF.withColumn(SABA_VALUE_COL,
                                 from_json(column(SABA_VALUE_COL), schemaST))
    val flattenDF = rdf1
      .select(
        flattenSchema(schemaST).map(c =>
          column("value." + c.toString.split(" AS ")(0).trim).alias(
            c.toString.split(" AS ")(0).trim)) ++ rawDF.columns.map(column): _*)
      .drop(SABA_VALUE_COL)
    flattenDF

  }

  def flattenSchema(schema: StructType,
                    delimiter: String = ".",
                    prefix: String = null): Array[Column] = {
    schema.fields.flatMap(structField => {
      val (codeColumName, colName) = prefix match {
        case _ if prefix == null => (structField.name, structField.name)
        case _ =>
          (prefix + "." + structField.name,
           prefix + delimiter + structField.name)
      }

      structField.dataType match {
        case st: StructType =>
          flattenSchema(schema = st, delimiter = delimiter, prefix = colName)
        case _ => Array(col(codeColumName).alias(colName))
      }
    })
  }

  def renameDFColumns(df: DataFrame): DataFrame = {
    try {
      df.columns.foldLeft(df) { (newDF, colName) =>
        newDF.withColumnRenamed(colName,
                                colName.replace(" ", "_").replace(".", "_"))
      }
    } catch {
      case _: Exception =>
        throw new Exception(
          s"Unable to rename Columns of the DataFrame: ${df.schema.mkString(",")}")
    }
  }

  def createCastedColumn(
      dfToBeCasted: DataFrame,
      flattenSchemaJsonMap: Map[String, String]): DataFrame = {
    var outputDF: DataFrame = dfToBeCasted
    try {
      flattenSchemaJsonMap.foreach { x =>
        val name = "`" + x._1 + "`"
        outputDF = x._2 match {
          case _ if x._2.equalsIgnoreCase("binary") =>
            outputDF.withColumn(s"afterCasting_${x._1}",
                                unbase64(dfToBeCasted(name)))
          case _ if x._2.equalsIgnoreCase("timestamp") =>
            outputDF.withColumn(s"afterCasting_${x._1}",
                                dfToBeCasted(name).cast("string"))
          case castTo =>
            outputDF.withColumn(s"afterCasting_${x._1}",
                                dfToBeCasted(name).cast(castTo))
        }
      }
    } catch {
      case _: Exception =>
        throw new Exception(
          s"Creating Casted Column, unknown dataType found in masterSchema")
    }
    outputDF
  }

  def renameSrcDFtoTargetDF(df: DataFrame,
                            columnMap: Map[String, String]): DataFrame = {
    columnMap.foldLeft(df)((df, col) => df.withColumnRenamed(col._1, col._2))
  }

  def currentTimeInMilliUTC: String = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    sdf.format(new Date())
  }

  def getOffsetString(df: DataFrame): String = {
    val offsetDf =
      df.groupBy(col("topic"), col("partition")).agg(max("offset").as("offset"))

    val listOfPartitionOffsetMap = offsetDf
      .sort(col("partition"))
      .map { x =>
        (x(0).toString, s""""${x(1)}":${x(2)}""")
      }
      .collect()
      .toList

    val offsetValue = listOfPartitionOffsetMap
      .groupBy(_._1)
      .map(x => x._2.reduce((x, y) => (x._1, x._2 + "," + y._2)))
      .toList
      .head
    "{\"" + offsetValue._1 + "\":{" + offsetValue._2 + "}}"
  }

  def getCommitEndOffset(endOffset: String,
                         endOffsetFromSource: String,
                         sourceName: String): String = {
    val endOffsetMap = OffsetUtility.mapFromOffsetString(endOffset)
    val endOffsetMapFromKafka =
      OffsetUtility.mapFromOffsetString(endOffsetFromSource)

    var commitEndOffsetMapFromDB: Map[Int, Long] = Map()

    val keyDifference = endOffsetMap.keySet.toList diff endOffsetMapFromKafka.keySet.toList
    if (keyDifference.nonEmpty) {
      keyDifference.foreach { key =>
        commitEndOffsetMapFromDB = commitEndOffsetMapFromDB ++ Map(
          key -> endOffsetMap(key))

      }
      commitEndOffsetMapFromDB = commitEndOffsetMapFromDB ++ endOffsetMapFromKafka
    } else
      commitEndOffsetMapFromDB = endOffsetMapFromKafka
    OffsetUtility.offsetStringFromMap(commitEndOffsetMapFromDB, sourceName)

  }

  def flattenJson(jsonMap:Map[String,Any])(
                 implicit tag:ClassTag[Map[String,Any]]
  ):Map[String,Any]={
    jsonMap.flatten{
      case (key,map:Map[String,Any])=>flattenJson(map.map(x=>(key+"_"+x._1,x._2)))
      case (key,value) => Map(key->value)
    }.toMap
  }

  def getInvalidNullRecords(inputDF:DataFrame,columnsToValidate:List[String]):DataFrame={
    var filterColumns = List[String]()
    columnsToValidate.foreach(r=>if(inputDF.columns.contains(r)){filterColumns=r::filterColumns})
    val filterCondition = filterColumns.map(c=>col(c).isNull).reduce(_ || _)
    inputDF.filter(filterCondition)
  }

}
