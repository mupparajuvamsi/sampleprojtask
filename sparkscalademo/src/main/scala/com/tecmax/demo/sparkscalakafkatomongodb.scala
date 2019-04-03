package com.tecmax.demo


import com.mongodb.spark.MongoSpark
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparkscalakafkatomongodb {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost/employeedb.employee")
      .config("spark.mongodb.output.uri", "mongodb://localhost/employeedb.employee")
      .getOrCreate()

    val sc=spark.sparkContext
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val kafkaParams = Map("bootstrap.servers" -> "localhost:9092",
    "group.id" -> "spark-demo",
      "auto.offset.reset" -> "largest",
      "kafka.consumer.id" -> "kafka-consumer-01"
    )
    /*val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      // used as i am using a string serializer for the input kafka topic
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-demo",
      "kafka.consumer.id" -> "kafka-consumer-01"
    )*/
    val topicName1 = List("test").toSet
    val topicName2 = List("test").toSet

    val stream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicName1)
    val lines1 = stream1.map(_._2)
    lines1.print()
    val elementDstream = stream1.map(_._2).foreachRDD { rdd =>
      //PeopleDf.write.format(mongoDbFormat).mode(SaveMode.Append).options(MongoDbOptiops).save()
      val employeeDataFrame=spark.read.json(rdd)
      MongoSpark.save(employeeDataFrame.write.option("collection", "employee").mode("append"))
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
