import org.apache.spark.sql.{SaveMode, SparkSession}
import java.util.Properties
object SparkScalaReadCsv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val employeeDataFrame = spark.read.format("csv")
      .option("header", "true")
      .load("C:\\Users\\Tecmax\\Desktop\\employee.csv")
    employeeDataFrame.show()

    val connectionProperties = new Properties()
    connectionProperties.put("user","root")
    connectionProperties.put("password","root")
    connectionProperties.setProperty("driver", "com.mysql.jdbc.Driver")
    employeeDataFrame.write
        .mode(SaveMode.Append)
      .jdbc("jdbc:mysql://localhost:3306/mysql","employee",connectionProperties)


  }


}
