version := "0.1"

scalaVersion := "2.12.12"

name := "Scala Demo"


libraryDependencies ++= Seq(
"org.apache.spark" % "spark-sql_2.12" % "2.4.6" % Provided,
"org.apache.spark" % "spark-mllib_2.12" % "3.2.1",
"org.apache.spark" % "spark-streaming_2.12" % "3.2.1",
"dev.ludovic.netlib" % "blas" % "2.2.1",
"dev.ludovic" % "blas" % "0.1.13",
"com.esotericsoftware" % "kryo" % "4.0.1"
)
