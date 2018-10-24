name := "spark_streaming_kafka"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0" % "provided"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.4.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-assembly" % "1.5.2"