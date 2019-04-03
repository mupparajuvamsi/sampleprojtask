package com.tecmax.demo

import org.apache.spark.sql.{SaveMode, SparkSession}

object sparkscalareadjson {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
      .master("local[*]")
      .getOrCreate()


    //read json data using sparksession
   val employeeDataFrame= spark.read
      .format("json")
      .load("src/main/resources/employee.json")

    employeeDataFrame.show(false)

    //write data to mysql table

    val url="jdbc:mysql://localhost:3306/sparkscala"

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    val table = "employee"

    employeeDataFrame.write
        .mode(SaveMode.Append)
      .jdbc(url,table,prop)
  }
}
