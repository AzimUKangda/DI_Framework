package com.test.framework.common

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types.StructType

import com.test.framework.common.Context.spark.implicits._
import com.test.framework.common.Constants._

object Utility {

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
}
