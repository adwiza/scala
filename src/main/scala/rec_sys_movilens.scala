import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd._

object rec extends App {


  val conf = new SparkConf()
    .setAppName("MovieLens recommendation system")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)



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

  val training = ratings.filter(x => x._1 < 6)
    .values
    .cache()
  val validation = ratings.filter(x => x._1 >= 6 && x._1 < 8)
    .values
    .cache()
  val test = ratings.filter(x => x._1 >= 8).values.cache()

  val numTraining = training.count()
  val numValidation = validation.count()
  val numTest = test.count()

  println {
    "Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest
  }

  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(f = x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  // Построение рекомендаций с ALS
  val ranks = List(8, 12)
  val lambdas = List(.1, 10)
  val numIterations = List(10, 20)
  var bestModel: Option[MatrixFactorizationModel] = None
  var bestValidationRmse = Double.MaxValue
  var bestRank = 0
  var bestLambda = -1.0
  var bestNumIter = -1
  for (rank <- ranks; lambda <- lambdas; numIteration <- numIterations) {
    val model = ALS.train(training, rank, numIteration, lambda)
    val validationRmse = computeRmse(model, validation, numValidation)
    println("RMSE (validator) = " + validationRmse + " for the model trained with rank = "
    + rank + ", lambda = " +  lambda + " and numIteration = " + numIteration + ".")
    if (validationRmse < bestValidationRmse) {
      bestModel = Some(model)
      bestValidationRmse = validationRmse
      bestRank = rank
      bestLambda = lambda
      bestNumIter = numIteration
    }
  }

// Проверка работы на рейтингах
  val testRmse = computeRmse(bestModel.get, test, numTest)
  println("The best model was trained waith rank = " + bestRank + " and lambda = " + bestLambda
    + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse +  ".")

  val meanRating = training.union(validation).map(_.rating).mean
  val baselineRmse =
    math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating)).mean)
  val improvement = (baselineRmse - testRmse) / baselineRmse * 100
  println("The best model improves the baseline by " + "%1.2f".format(improvement) + "% .")

  val candidates = sc.parallelize(movies.keys.toSeq)
  val recomendations = bestModel.get
    .predict(candidates.map((100, _)))
    .sortBy(- _.rating)
    .take(10)
  var i = 1
  println("Movies recomended for you:")
  recomendations.foreach {r =>
    println("%2d".format(i) + ": " + movies(r.product))
    i += 1
  }
}