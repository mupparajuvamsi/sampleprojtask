import org.apache.spark.sql.SparkSession

object sparkscalareadmultilienjson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
   
  }
}
