import org.apache.spark.sql.SparkSession

case class Crime (
                   INCIDENT_NUMBER: String,
                   OFFENSE_CODE: Int,
                   OFFENSE_CODE_GROUP: String,
                   OFFENSE_DESCRIPTION: String,
                   DISTRICT: String,
                   REPORTING_AREA: String,
                   SHOOTING: String,
                   OCCURRED_ON_DATE: String,
                   YEAR: Int,
                   MONTH: Int,
                   DAY_OF_WEEK: String,
                   HOUR: Int,
                   UCR_PART: String,
                   STREET: String,
                   Lat: Double,
                   Long: Double,
                   Location: String
                 ) {

  def wasShooting: Boolean = { SHOOTING != "null" }

}

object HWApp extends App {

  val spark = SparkSession.builder()
    .master( master = "local[*]")
    .getOrCreate()

  import spark.implicits._

  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
//    .csv("/home/adwiz/Python/jupyter_notebooks/crime.csv")
    .csv("/home/adwiz/tmp/salestk_202201211420.csv")

//  crimeFacts.as[Crime].filter(x => x.OFFENSE_DESCRIPTION == "VANDALISM")
//  crimeFacts.as[Crime].filter(x => x.wasShooting)
  crimeFacts.show(numRows = 20, truncate = false)
}
//  crimeFacts.as[Crime]
//    .show(numRows = 20, truncate = false)
//}
