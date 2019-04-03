package com.tecmax.demo

import org.apache.spark.sql.{SaveMode, SparkSession}

object sparkscalareadexcel {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val employeeDataFrameExcel=spark.read

      .format("com.crealytics.spark.excel")
      .option("location", "C:\\Users\\Tecmax\\Desktop\\emp.xlsx")
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .option("sheetName", "Sheet1")
      .load("C:\\Users\\Tecmax\\Desktop\\emp.xlsx")

    //employeeDataFrameExcel.show(false)
    val url="jdbc:mysql://localhost:3306/sparkscala"

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    val table = "employee"

    employeeDataFrameExcel.write
      .mode(SaveMode.Append)
      .jdbc(url,table,prop)

  }
}
