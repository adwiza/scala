version := "0.1"

scalaVersion := "2.13.10"

lazy val sparkVersion = "3.3.1"
//lazy val kafkaVersion = "2.7.6"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.2",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.13" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % Provided,
  "org.apache.kafka" % "kafka-clients" % sparkVersion
)

lazy val root = (project in file("."))
  .settings(
    name := "crispdm"
  )
