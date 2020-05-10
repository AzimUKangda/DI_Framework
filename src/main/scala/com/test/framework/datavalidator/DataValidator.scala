package com.test.framework.datavalidator

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.udf

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

  

}
