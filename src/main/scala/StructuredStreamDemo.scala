import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


class StructuredStreamDemo {
  /*val sparkSession = SparkSession.builder
    .master("local")
    .appName("my-spark-app")
    .getOrCreate()
  // .config("spark.some.config.option", "config-value")

  //sparkSession.read.stream()
 // val all = sparkSession.read.format("json").stream("data/stocks.json")


  //is there a structured stream for twitter
  //how to ad-hoc query twitter?
*/
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  import spark.implicits._
  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count()

  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()

  //scala wordcount
  val words = ""
  words.split(" ")
    .groupBy(word => word)
    .mapValues(value => value.size)

  //rdd wordcount
  val sc = new SparkContext()
  val input = sc.textFile("data/all-shakespeare.txt")
  val lines = input.map(line => line.toLowerCase)
  val wordCount = lines
      .flatMap(line => line.split(" "))
      .groupBy(word => word)
      .mapValues(value => value.size)



}
