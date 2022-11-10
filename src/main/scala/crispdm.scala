import org.apache.spark.ml.feature.Intercept
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
//  # Рабрта с признаками
//  Проверяем корреляции чисовых признаков
  val pairs = numericColumns
  .flatMap(f1 => numericColumns.map(f2 => (f1, f2)))
  .filter { p => !p._1.equals(p._2) }
  .map { p => if (p._1 < p._2) (p._1, p._2) else (p._2, p._1) }
  .distinct

  val corr = pairs
    .map { p => (p._1, p._2, data.stat.corr(p._1, p._2)) }
    .filter(_._3 > .6)

  corr.sortBy(_._3).reverse.foreach { c => println(f"${c._1}%25s${c._2}%25s\t${c._3}") }
  var numericColumnsFinal = numericColumns.diff(corr.map(_._2))
  println(numericColumnsFinal)

//  Категориальные признаки
//  Индексируем строковые колонки
   import org.apache.spark.ml.feature.StringIndexer

  val stringColumns = data
    .dtypes
    .filter(_._2.equals("StringType"))
    .map(_._1)
    .filter(!_.equals("Attrition_Flag"))

  val stringColumnsIndexed = stringColumns.map(_ + "_Indexed")
  val indexer = new StringIndexer()
    .setInputCols(stringColumns)
    .setOutputCols(stringColumnsIndexed)

  val indexed = indexer.fit(data).transform(data)
  indexed.show(5 )

//  Кодируем категориальные признаки
  import org.apache.spark.ml.feature.OneHotEncoder

  val catColumns = stringColumnsIndexed.map(_ + "_Coded")
  val encoder = new OneHotEncoder()
    .setInputCols(stringColumnsIndexed)
    .setOutputCols(catColumns)

  val encoded = encoder.fit(indexed).transform(indexed)
  encoded.show(5)
//  Собираем признаки в вектор
  import org.apache.spark.ml.feature.VectorAssembler

  val featureColumns = numericColumnsFinal ++ catColumns

  val assembler = new VectorAssembler()
    .setInputCols(featureColumns)
    .setOutputCol("features")

  val assembled = assembler.transform(encoded)
  assembled.show(5, truncate = false)

//  Нормализация
  import org.apache.spark.ml.feature.MinMaxScaler

  val scaler = new MinMaxScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")

  val scaled = scaler.fit(assembled).transform(assembled)
  scaled.show(5, truncate = false)
//  Feature Selection (Отбор признаков)
  import org.apache.spark.ml.feature.ChiSqSelector

  val selector = new ChiSqSelector()
    .setNumTopFeatures(10)
    .setFeaturesCol("scaledFeatures")
    .setLabelCol("target")
    .setOutputCol("selectedFeatures")
  val dataF = selector.fit(scaled).transform(scaled)
  dataF.show(5, truncate = false)

//  Моделирование
//  Обучающая и тестовая выборки

  val tt = dataF.randomSplit(Array(.7, .3))
  val training = tt(0)
  val test = tt(1)

  println(s"training\t${training.count()}\ntest\t${test.count()}")
//  Логистическая регрессия
  import org.apache.spark.ml.classification.LogisticRegression

  val lr = new LogisticRegression()
    .setMaxIter(1000)
    .setRegParam(.2)
    .setElasticNetParam(.8)
    .setFeaturesCol("selectedFeatures")
    .setLabelCol("target")

  val lrModel = lr.fit(training)

  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

//  Training Summary

  val trainingSummary = lrModel.binarySummary
  println(s"accuracy: ${trainingSummary.accuracy}")
  println(s"areaUnderROC: ${trainingSummary.areaUnderROC}")

//  Проверяем модель на тестовой выборке
  val predicted = lrModel.transform(test)
  predicted.select("target", "rawPrediction", "probability", "prediction")
    .show(10, truncate=false)

  import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

  val evaluator = new BinaryClassificationEvaluator().setLabelCol("target")
  println(s"areaUnderROC: ${evaluator.evaluate(predicted)}\n")

//  Confusion Matrix (матрица ошибок)
// TP - true positive
// TN - true negative
// FP - false positive
// FN - false negative

  val tp = predicted.filter(($"target" === 1) and ($"prediction") === 1).count()
  val tn = predicted.filter(($"target" === 0) and ($"prediction") === 0).count()
  val fp = predicted.filter(($"target" === 0) and ($"prediction") === 1).count()
  val fn = predicted.filter(($"target" === 1) and ($"prediction") === 0).count()

  println(s"Confusion Matrix:\n$tp\t$fp\n$fn\t$tn\n")

//  Accuracy, Precision, Recall
  val accuracy = (tp + tn) / (tp + tn + fp + fn).toDouble
  val precision = tp / (tp + fp).toDouble
  val recall = tp / (tp + fn).toDouble

  println(s"Accuracy = $accuracy")
  println(s"Precision = $precision")
  println(s"Recall = $recall")

//  Настаиваем модель, подбираем гиперпараметры

  import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

  val paramGrid = new ParamGridBuilder()
    .addGrid(lr.regParam, Array(.01, .1, .5))
    .addGrid(lr.fitIntercept)
    .addGrid(lr.elasticNetParam, Array(.0, .5, 1.0))
    .build()

  val trainValidationSplit = new TrainValidationSplit()
    .setEstimator(lr)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(paramGrid)
    .setTrainRatio(.7)
    .setParallelism(2)

  val model = trainValidationSplit.fit(dataF)

  model.bestModel.extractParamMap()

  val bestML = model.bestModel
  /*
  val bestML = new LogisticRegression()
          .setMaxIter(1000)
          .setRegParam(.01)
          .setElasticNetParam(.0)
          .setFeaturesCol("selectedFeatures")
          .setLabelCol("target")
   */
//  Собираем все вместе (Pipeline)
    /*
    1. Отобрали числовые признаки: NumericColumnsFinal
    2. Проиндексировали строковые призкаки: indexer
    3. Закодировали категориальные признаки: encoder
    4. Собрали признаки в вектор: assembler
    5. Нормализовали признаки: scaler
    6. Провели отбор признаков: selector
    7. Рассчитали модель: bestML
    */
   import org.apache.spark.ml.Pipeline

  val pipeline = new Pipeline().setStages(Array(indexer, encoder, assembler, scaler, selector, bestML))

  val ttData = data.randomSplit(Array(.7, .3))
  val trainingData = ttData(0)
  val testData = ttData(1)

  val pipelineModel = pipeline.fit(trainingData)

//  Сохраняем модель
  pipelineModel.write.overwrite().save("/Users/adwiz/IdeaProjects/crispdm/pipelineModel")

}

