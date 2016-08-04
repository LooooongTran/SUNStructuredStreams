import org.apache.spark.{SparkConf, SparkContext, streaming}
import org.apache.spark.sql.SparkSession


class StructuredStreamDemo {

  val spark = SparkSession
    .builder
    .appName("StructuredStreamWordCount")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  val lines = spark.readStream.text("inputDir/")

  val words = lines.as[String].flatMap(_.split(" "))

  val wordCounts = words.groupBy("value").count()

  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()





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


  //structured stream word count
 /* import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local")
    .getOrCreate()

  import spark.implicits._
  // Create DataFrame representing the stream of input lines from connection to localhost:9999
  val lines = spark.readStream.text("inputDir/")
    //.format("socket")
    //.option("host", "localhost")
    //.option("port", 9999)
    //.load()

  // Split the lines into words
  val words = lines.as[String].flatMap(_.split(" "))

  // Generate running word count
  val wordCounts = words.groupBy("value").count()

  // Start running the query that prints the running counts to the console
  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()*/

  //scala wordcount
 /* val words = ""
  val wordCount = words.toLowerCase
    .split(" ")
    .groupBy(word => word)
    .mapValues(value => value.length)
*/

  //rdd wordcount

 /* val sc = new SparkContext()
  val words = sc.textFile("all-shakespeare.txt")

  val wordCount = words.map(line => line.toLowerCase)
      .flatMap(line => line.split(" "))
      .groupBy(word => word)
      .mapValues(value => value.size)
*/


  //dataframes wordcount
/*
  val linesDF = spark.sparkContext.textFile("data/all-shakespeare.txt").toDF("line")
    linesDF.createOrReplaceTempView("lineDF")
  val wordsDF = linesDF.select(explode(split('line, " ")).as("word"))
  val wordCountDF = wordsDF.groupBy("word").count()
  wordCountDF.show()
*/
  //sql wordcount
 /* linesDF.createOrReplaceTempView("lineDF")
  val wordCountSQL = spark.sql(
      """select substr(line,1,(instr(line," ")-1)) AS word,  count(*) AS count
        |   from lineDF
        |   group by  substr(line,1,(instr(line," ")-1))
        |   order by count desc""".stripMargin)
  wordCountSQL.show*/

  //DStream word count
/*  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.streaming.dstream.DStream
  import org.apache.spark.streaming.{Seconds, StreamingContext}

  val sparkConf = new SparkConf().setMaster("local").setAppName("DF wordcount")
  val sc = new SparkContext(sparkConf)
  val ssc = new org.apache.spark.streaming.StreamingContext(sc, Seconds(10))
  ssc.checkpoint("checkpointDirectory")

  val incomingFiles = ssc.textFileStream("inputDir/")

  val wordCount  =  incomingFiles.flatMap(x => x.split(" "))
                            .map(word => (word, 1))
                            .reduceByKeyAndWindow(reduceFunc = _ + _,
                              invReduceFunc = _ - _,
                              slideDuration = Seconds(10),
                              windowDuration = Seconds(30))
                            .transform(_.sortBy(_._2, ascending = false))

  wordCount.foreachRDD(x => x.foreach(y => println(s"this is inside an RDD $y")))
  wordCount.print(20)
  ssc.start()
  ssc.awaitTermination()

  ssc.stop(stopSparkContext = true)
*/
}
