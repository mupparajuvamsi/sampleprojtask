package com.tecmax.demo

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.SparkSession

object sparkscalawritetomongodb {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://52.87.206.150/employeedb.employee")
      .config("spark.mongodb.output.uri", "mongodb://52.87.206.150/employeedb.employee")
      .getOrCreate()

    /*val studentDataFrame=spark.read
      .format("json")
      .load("src/main/resources/student.json")*/
    val writeConfig = WriteConfig(Map("collection" -> "student", "writeConcern.w" -> "majority"), Some(WriteConfig(spark.sparkContext)))
    //MongoSpark.save(studentDataFrame, writeConfig)

    val readConfig = ReadConfig(Map("collection" -> "student", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(spark.sparkContext)))
    val customRdd = MongoSpark.load(spark.sparkContext, readConfig)
    customRdd.collect().foreach(println)



  }
}
