package com.tecmax.demo

import java.util.Properties

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

object sparkscalahelloworld {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local")
      .config("spark.mongodb.input.uri", "mongodb://34.239.101.16/test.myCollection")
      .config("spark.mongodb.output.uri", "mongodb://34.239.101.16/test.myCollection")
      .getOrCreate()

    val sc=spark.sparkContext

    val employeeDataFramecsv=spark.read
      .format("csv")
      .option("header","true")
      .load("C:\\Users\\Tecmax\\Desktop\\employee.csv")
    employeeDataFramecsv.printSchema()
    //employeeDataFramecsv.show()


   /* val employeeDataFramejson=spark.read
      .format("json")
      .option("header","true")
      .load("src/main/resources/employee.json")

    employeeDataFramejson.printSchema()
    employeeDataFramejson.show(false)*//* val employeeDataFramejson=spark.read
      .format("json")
      .option("header","true")
      .load("src/main/resources/employee.json")

    employeeDataFramejson.printSchema()
    employeeDataFramejson.show(false)*/
    val conn=new Properties()
    /*conn.setProperty("username","root")
    conn.setProperty("password")
    employeeDataFramejson.write
      .jdbc("jdbc://mysql://localhost:3306/sparkscala","employee")*/

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    //jdbc mysql url - destination database is named "data"
    val url = "jdbc:mysql://localhost:3306/sparkscala"

    //destination database table
    val table = "employee"

    //write data from spark dataframe to database
    //employeeDataFramecsv.write.mode("append").jdbc(url, table, prop)

    MongoSpark.save(employeeDataFramecsv)
  }
}
