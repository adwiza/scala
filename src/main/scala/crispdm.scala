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
    val colsToDrop: Array[String] = columns.slice(columnsLen - 2, columnsLen) ++ Array("CLIENTNUM")

    val df = raw.drop(colsToDrop: _*)
//    val df = raw.drop("CLIENTNUM")
    df.show(5, truncate = false)

  //  Detecting columns type

  df.printSchema

  df.dtypes.groupBy(_._2).mapValues(_.length)

//  Проверяем числовые колонки
  val numericColumns = df.dtypes.filter(!_._2.equals("StringType")).map(d => d._1)
  df.select(numericColumns.map(col): _*).summary().show

  df.groupBy($"Customer_Age").count().show(100)

// Целевая колонка
  val dft = df.withColumn("target", when($"Attrition_Flag" === "Existing Customer", 0)
    .otherwise(1))
  dft.show(5, truncate = false)

// Несбалансированное распределение данных
  dft.groupBy("target").count().show()

//  Oversampling
  val df1 = dft.filter($"target" === 1)
  val df0 = dft.filter($"target" === 0)

  val df1count = df1.count()
  val df0count = df0.count()

  println(df0count / df1count)

  val df1Over = df1
    .withColumn("dummy", explode(lit((1 to (df0count / df1count).toInt).toArray)))
    .drop("dummy")

  df1Over.show(10, truncate = false)

  val data = df0.unionAll(df1Over)
  data.groupBy("target").count().show()
}

