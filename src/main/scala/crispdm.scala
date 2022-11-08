import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object crispdm extends App {

  val spark = SparkSession
    .builder()
    .appName("CRISP-DM")
    .master("local")
    .getOrCreate()

  import spark.implicits._


  //  Load data
  val raw = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/Users/adwiz/IdeaProjects/crispdm/BankChurners-1801-1d994f.csv")

    val columns: Array[String] = raw.columns
    val columnsLen: Int = columns.length
//    val colsToDrop: Array[String] = columns.slice(columns - 2, columnsLen) ++ Array("CLIENTNUM")

//    val df = raw.drop(colsToDrop: _*)
    val df = raw.drop("CLIENTNUM")
    df.show(5, truncate = false)

  //  Detecting columns type

  df.printSchema

  df.dtypes.groupBy(_._2).mapValues(_.length)


}

