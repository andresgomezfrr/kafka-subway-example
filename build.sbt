name := "kafka-exercise"

version := "0.1"

scalaVersion := "2.12.10"

val circeVersion = "0.12.3"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.11.3",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.6.0" % Test
)
