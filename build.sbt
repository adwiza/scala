version := "0.1"

scalaVersion := "2.12.12"

name := "Scala Demo"
val sparkVer = "3.2.1"

libraryDependencies ++= Seq(
"org.apache.spark" % "spark-sql_2.12" % "2.4.6" % Provided,
"org.apache.spark" % "spark-mllib_2.12" % "3.2.1",
"com.esotericsoftware" % "kryo" % "5.2.1"
)
