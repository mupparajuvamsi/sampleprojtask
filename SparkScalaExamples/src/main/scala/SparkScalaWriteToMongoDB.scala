import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
object SparkScalaWriteToMongoDB {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MongoSparkConnectorIntro")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sparkscalatest.empcollection")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sparkscalatest.empcollection")
      .getOrCreate()
    val employeeJsonSchema=StructType(
      Seq(
        StructField("empid", StringType, true),
        StructField("empname", StringType, true),
        StructField("empsal", StringType, true)
      )
    )

    val employeeDataFrame=spark.read.format("json").schema(employeeJsonSchema).load("src/main/resources/json-resources/employee1.json")
   // employeeDataFrame.show()

    val employeeWriteConfig = WriteConfig(Map("collection" -> "employee", "writeConcern.w" -> "majority"), Some(WriteConfig(spark.sparkContext)))
    //MongoSpark.save(employeeDataFrame,employeeWriteConfig)
    val readConfig = ReadConfig(Map("collection" -> "employee", "writeConcern.w" -> "majority"),Some(ReadConfig(spark.sparkContext)))
    val employeeDataFrameFromMongoDB = MongoSpark.load(spark,readConfig)
    employeeDataFrameFromMongoDB.show()
  }
}
