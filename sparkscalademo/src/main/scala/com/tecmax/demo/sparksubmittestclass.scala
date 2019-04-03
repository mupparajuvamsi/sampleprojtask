package com.tecmax.demo

import org.apache.spark.sql.SparkSession

object sparksubmittestclass {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAJP7GLCAC7LHJVCAA")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
    val employeeDataFrame = spark.read.format("csv").option("header", "true").load("s3a://user2bucket1/employee.csv")
    employeeDataFrame.show()
    print("sparksubmittestclass....................*************sparksubmittestclass")

  }
}
