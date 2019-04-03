import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark

object SparkScalaElasticSearch {
  def main(args: Array[String]): Unit = {
    // define a case class
    val conf = new SparkConf().setAppName("SparkScalaElasticSearch").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")
    case class Employee(empid: String, empname: String,empsal: String)
    val employeeVamsi = Employee("100", "vamsi","1000")
    val employeeKrishna = Employee("200", "krishna","2000")
    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(Seq(employeeVamsi, employeeKrishna))
    EsSpark.saveToEs(rdd, "spark/docs")




  }
}
