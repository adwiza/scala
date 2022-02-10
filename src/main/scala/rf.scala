import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

object rf extends App {
  val conf = new SparkConf()
    .setAppName("Random Forest classifier")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  // Загрузка и парсинг данных
  val data = MLUtils.loadLibSVMFile(sc, "data.txt")
//  Разделение данных на train  test
  val splits = data.randomSplit(Array(.7, .3))
  val (trainingData, testData) = (splits(0), splits(1))
//  Тренировка модели
  val treeStrategy = Strategy.defaultStrategy("Classification")
  val numTrees = 200
  val featureSubsetStrategy = "auto"
  val model = RandomForest.trainClassifier(trainingData, treeStrategy, numTrees, featureSubsetStrategy, seed = 12345)
//  Проверка на тест сете и подсчет ошибки
  val testErr = testData.map{point => val prediction = model.predict(point.features)
  if (point.label == prediction) 1.0 else 0.0}.mean()
  println("Test Error=" + testErr)
  println("Learned Randon Forest:n" + model.toDebugString)
}