package com.companyname.cassandrademo
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.SomeColumns
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object WriteDataToCassandra {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()
    import spark.implicits._
    val newCourses = Seq(Row("CS011", "ML - Advance"), Row("CS012", "Sentiment Analysis"))
    val courseSchema = List(StructField("cid", StringType, true), StructField("cname", StringType, true))
    val newCoursesDF = spark.createDataFrame(spark.sparkContext.parallelize(newCourses), StructType(courseSchema))
    val collection = spark.sparkContext.parallelize(Seq(("CS011", "ML - Advance"), ("CS012", "Sentiment Analysis")))
    //collection.saveToCassandra("test", "words", SomeColumns("word", "count"))
  }
}
