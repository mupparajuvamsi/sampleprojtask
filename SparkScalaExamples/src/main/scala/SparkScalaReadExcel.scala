import org.apache.spark.sql.{SaveMode, SparkSession}
object SparkScalaReadExcel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val employeeDataFrame = spark.read
      .format("com.crealytics.spark.excel")
      .option("location", "C:\\Users\\Tecmax\\Desktop\\emp.xlsx")
      .option("useHeader", "true")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .load("C:\\Users\\Tecmax\\Desktop\\emp.xlsx")
    employeeDataFrame.show()

    val postgresqlConnectionProperties=new java.util.Properties()
    postgresqlConnectionProperties.put("username","postgres")
    postgresqlConnectionProperties.put("password","postgres")
       System.setProperty("driver","org.postgresql.Driver")


    employeeDataFrame.write
      .mode(SaveMode.Append)
      .jdbc("jdbc:postgresql://localhost:5432/sparkscala","sparkscalaexcelemployee",postgresqlConnectionProperties)




  }


}
