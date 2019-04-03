import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkScalaStructuredStreamingMongoDB {
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
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sparkscalatest.empcollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sparkscalatest.empcollection")
      .appName("sample-structured-streaming")
      .getOrCreate()
  }
}
