import breeze.linalg
import breeze.linalg._
import breeze.numerics._
import breeze.optimize.{DiffFunction, LBFGS}
import breeze.stats.mean
//import dev.ludovic.netlib.NativeBLAS
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.attribute.AttributeGroup

object breeze_start extends App {
  val spark = SparkSession.builder()
    .master( master = "local[*]")
    .getOrCreate()


  //  println(NativeBLAS.getInstance())
  //  Создание объектов
//  println(DenseMatrix.zeros[Double](3, 5))
//  println(DenseVector.fill(3) {
//    5.0
//  })
//  println(DenseMatrix((1.0, 2.0), (3.0, 4.0)))
//
//  println(linspace(2, 7, 4))
//  println(DenseMatrix.tabulate(3, 2) { case (i, j) => i + j })
//  println(DenseMatrix.rand(2, 3))
//  //  Слайсинг и индексация
//  val m = DenseMatrix.tabulate(3, 3) { case (i, j) => i - j }
//  val cat = m(::, 1 to 2)
//  val e = m(-1, 1)
//  val v = DenseVector(1.0, 2.0, 3.0)
//  val A = DenseMatrix((1.0, 2.0, 3.0), (1.0, 0.0, 2.0))
//  println(A * v)
//  println(v dot v)
//  println(v.t * v)
//  println(A * A.t)
//
//
//  //  Поэлементные операции
//  val vv = DenseVector(1.0, 2.0, 3.0)
//  println(vv *:* vv)
//  vv :+= vv
//  println(vv)
//  println(vv :*= 3.0)
//  println(vv <:< (vv +:+ vv))
//  println(argmax(vv))
//  //  Агрегаты
//  val AA = DenseMatrix((1.0, 2.0, 3.0), (1.0, 0.0, 2.0))
//  println(sum(AA))
//  println(mean(AA(::, *)))
//  println(trace(AA(::, 0 to 1)))
//  println(accumulate(AA.toDenseVector))
//  //  Линейная алгебра
//  val AAA = DenseMatrix((1.0, 2.0), (5.0, 3.0))
//  val b = AAA * DenseVector(3.0, 1.0)
//
//  println(AAA \ b)
//  println(det(AAA))
//  println(inv(AAA))
//  println(eig(AAA))
//
//  val X = DenseMatrix.rand(2000, 3)
//  val y = X * DenseVector(0.5, -0.1, 0.2)
//
//  val J = new DiffFunction[DenseVector[Double]] {
//    def calculate(w: DenseVector[Double]) = {
//      val e = X * w - y
//      val loss = sum(e ^:^ 2.0) / (2 * X.rows)
//      val grad = (e.t * X) /:/ (2.0 * X.rows)
//      (loss, grad.t)
//    }
//  }
//
//  val optimizer = new LBFGS[DenseVector[Double]]()
//  println(optimizer.minimize(J, DenseVector(0.0, 0.0, 0.0)))
  import spark.implicits._

  val X = DenseMatrix.rand(100000, 3)
  val y = X * DenseVector(0.5, -0.1, 0.2)
  val data = DenseMatrix.horzcat(X, y.asDenseMatrix.t)

  val df = data(*, ::).iterator
    .map(x => (x(0), x(1), x(2), x(3)))
    .toSeq.toDF("x1", "x2", "x3", "y")

  println(df.show(1))

  val pipeline = new Pipeline().setStages(Array(
      new VectorAssembler()
          .setInputCols(Array("x1", "x2", "x3"))
          .setOutputCol("features"),
      new LinearRegression().setLabelCol("y")
  ))
  val model = pipeline.fit(df)

  val w = model.stages.last.asInstanceOf[LinearRegressionModel].coefficients
  val predictions = model.transform(df)
  predictions.show(10)
  predictions.printSchema

  AttributeGroup.fromStructField(predictions.schema("features"))
    .attributes.get.foreach(println)
}