import org.apache.spark.ml.feature.Intercept
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object crispdmProd extends App {

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

}
