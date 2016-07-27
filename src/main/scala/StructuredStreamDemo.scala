import org.apache.spark.sql.SparkSession


class StructuredStreamDemo {
  val sparkSession = SparkSession.builder
    .master("local")
    .appName("my-spark-app")
    .getOrCreate()
  // .config("spark.some.config.option", "config-value")

  //sparkSession.read.stream()
  val all = sparkSession.read.format("json").stream("data/stocks.json")


  //is there a structured stream for twitter
  //how to ad-hoc query twitter?
}
