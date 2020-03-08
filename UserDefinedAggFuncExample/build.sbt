name := "UserDefinedAggFuncExample"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.5", // Spark Core Library
  "org.scalatest" %% "scalatest" % "3.0.5" % "test" // Scala test library
)