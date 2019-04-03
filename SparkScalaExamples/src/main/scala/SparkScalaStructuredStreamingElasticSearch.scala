import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions

object SparkScalaStructuredStreamingElasticSearch {
  def main(args: Array[String]): Unit = {
    val jsonSchema = StructType(
      Seq(
        StructField("empid", StringType, true),
        StructField("empname", StringType, true),
        StructField("empsal", StringType, true)
      )
    )
    val conf = new SparkConf().setAppName("SparkScalaElasticSearch").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")
    val sparkSession = SparkSession.builder()
      .master("local[*]")
     // .config(ConfigurationOptions.ES_NODES, "127.0.0.1")
     // .config(ConfigurationOptions.ES_PORT, "9200")
      .config(conf)
      .appName("sample-structured-streaming")
      .getOrCreate()
    val streamingDF: DataFrame = sparkSession
      .readStream
      .schema(jsonSchema)
      .json("src/main/resources/json-resources/")



    streamingDF.writeStream
      .outputMode("append")
      .format("org.elasticsearch.spark.sql")
      .option("checkpointLocation", "src/main/resources/path-to-checkpointing")
      .start("index-name/doc-type").awaitTermination()


  }
}
