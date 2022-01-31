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

object rec extends App {

//  val spark = SparkSession.builder()
//    .master( master = "local[*]")
//    .getOrCreate()

//  import spark.implicits._

  val conf = new SparkConf()
    .setAppName("Simple Application")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

//  val crimeFacts = spark
//    .read
//    .option("header", "true")
//    .option("inferSchema", "true")

  //  Загрузка данных и парсинг
  val moveLensHomeDir = "/home/adwiz/Python/DATA/ml-10M100K/"
  val movies = sc.textFile(moveLensHomeDir + "movies.dat").map { line =>
    val fields = line.split("::")
//    format: (movieId, movieName)
    (fields(0).toInt, fields(1))
  }.collect.toMap

  val ratings = sc.textFile(moveLensHomeDir + "ratings.dat").map { line =>
    val fields = line.split("::")
//    format: (timestamp % 10, Rating(userId, movieId, rating))
    (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
  }

  val numRatings = ratings.count
  val numUsers = ratings.map(_._2.user).distinct.count
  val numMovies =ratings.map(_._2.product).distinct.count

  println("Got " + numRatings + " ratings from "
    + numUsers + " users on " + numMovies + " movies.")

// Построение рекомендаций с ALS
//  val rank = 10
//  val numIterations = 20
//  val model = ALS.train(ratings, rank, numIterations, 0.01)

// Проверка работы на рейтингах
//  val usersProducts = ratings.map { case Rating(user, product, rate) =>
//  (user, product)
//  }
//  val predictions = model.predict(usersProducts)
}