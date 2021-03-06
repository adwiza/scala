import java.io.File
import scala.io.Source

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

object Main extends App {

  val spark = SparkSession.builder()
    .master( master = "local[*]")
    .getOrCreate()

  import spark.implicits._
  val conf = new SparkConf().setAppName("Simple Application")
  val sc = new SparkContext(conf)
  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
//    .csv("/home/adwiz/Python/jupyter_notebooks/crime.csv")
//    .csv("/home/adwiz/tmp/salestk_202201211420.csv")

//  Загрузка данных и парсинг
  val data = sc.textFile("test.data")
  val ratings = data.map(_.split(',') match {
      case Array(user, item, rate) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
  })
// Построение рекомендаций с ALS
  val rank = 10
  var numIterations = 20
  val model = ALS.train(ratings, rank, numIterations, 0.01)

// Проверка работы на рейтингах
  val usersProducts = ratings.map { case Rating(user, product, rate) =>
  (user, product)
  }
  val predictions = model.predict(usersProducts)
  }