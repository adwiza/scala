version := "0.1"

scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.2" % Provided,
  "org.apache.spark" %% "spark-sql" % "3.3.1" % Provided
)

lazy val root = (project in file("."))
  .settings(
    name := "crispdm"
  )
