import org.apache.spark.sql.SparkSession


class StructuredStreamDemo {
  val sparkSession = SparkSession.builder
    .master("local")
    .appName("my-spark-app")
   // .config("spark.some.config.option", "config-value")
    .getOrCreate()


}
