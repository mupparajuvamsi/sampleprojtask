package com.tecmax.demo

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark

object sparkscalaelasticsearch {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]")
      //.config("spark.mongodb.input.uri", "mongodb://52.87.206.150/employeedb.employee")
      //.config("spark.mongodb.output.uri", "mongodb://52.87.206.150/employeedb.employee")
      .config("es.index.auto.create", "true")
      .config("es.nodes", "localhost:9200")
      .config("spark.couchbase.nodes", "3.86.111.103")
      //.config("spark.couchbase.nodes", "127.0.0.1") // connect to couchbase on hostname
      //.config("spark.couchbase.bucket.employeedb","") // open the travel-sample bucket with empty password
      .config("com.couchbase.username", "vamsikrishna")
      .config("com.couchbase.password", "Chak#436")
      .config("com.couchbase.kvTimeout","10000")
      .config("com.couchbase.connectTimeout","30000")
      .config("com.couchbase.socketConnect","10000")
    //.config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    import spark.implicits._

    val employees = Map("empid" -> "100", "empname" -> "vamsi")

   //val emprdd= spark.sparkContext.makeRDD(Seq(employees))

    //val dfWithoutSchema = spark.createDataFrame(emprdd).toDF("id", "vals")

    val someDF = Seq(
      ("100","vamsi")

    ).toDF("empid", "empname")

   // someDF.show(false)
   // define a case class
  /* case class Employee(empid: String, empname: String)

    val emp1 = Employee("emp1", "SFO")
    val emp2 = Employee("emp2", "OTP")

    val rdd = spark.sparkContext.makeRDD(Seq(emp1, emp2))

    EsSpark.saveToEs(rdd, "employeeindex/employeedoc")
  */  //read json data using sparksession
    val employeeDataFrame= spark.read
      .format("json")
      .load("src/main/resources/employee.json")

    val rddfromdataframe=employeeDataFrame.rdd

    //EsSpark.saveToEs(rddfromdataframe, "employeeindex/employeedoc")

  /*  employeeDataFrame.write.format("org.elasticsearch.spark.sql") \
    .mode('append') \
      .option("es.resource", "log/raw") \
    .option("es.nodes", "localhost").save("log/raw")*/

    /*employeeDataFrame.write
      .format("org.elasticsearch.spark.sql")
      .mode(SaveMode.Append)
      .save("employeeindex/employeedoc")*/

    //3889481fe1ffb11adc109342c5000bd1

    spark.sparkContext
      .couchbaseGet[JsonDocument](Seq("3889481fe1ffb11adc109342c5000bd1")) // Load documents from couchbase
      .collect() // collect all data from the spark workers
      .foreach(println) // print each document content
    println("=====================================================================================")


  }
}
