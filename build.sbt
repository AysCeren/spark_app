ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"
lazy val root = (project in file("."))
  .settings(
    name := "SparkDataManipulator"
  )
organization := "databoss"
autoScalaLibrary := false

//Spark
val sparkVersion ="3.5.1"
// adding dependencies
val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % sparkVersion
)
libraryDependencies ++= sparkDependencies
