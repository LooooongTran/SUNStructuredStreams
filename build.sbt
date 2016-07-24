name := "SUNStructuredStreams"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "2.0.0-preview",
  "org.apache.spark" % "spark-streaming_2.10" % "2.0.0-preview",
  "org.apache.spark" % "spark-sql_2.10" % "2.0.0-preview"
)

