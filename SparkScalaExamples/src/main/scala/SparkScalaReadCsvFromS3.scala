import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkScalaReadCsvFromS3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AKIAJT4T6OPJZAQLMEKA")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
    val employeeDataFrame = spark.read.format("csv").option("header", "true").load("s3a://testdemoawsweekeend/employee.csv")
    // employeeDataFrame.show()

    // Subscribe to a topic and read messages from the earliest to latest offsets
    val ds= spark
      .readStream // use `read` for batch, like DataFrame
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe", "mytopic")
      .option("startingOffsets", "earliest")
      .load()
    import spark.sqlContext.implicits._
    val dsStruc = ds.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    dsStruc.show()



  }
}
