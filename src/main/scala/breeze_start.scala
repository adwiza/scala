import breeze.linalg._
import breeze.numerics._
import breeze.stats.mean
//import dev.ludovic.netlib.NativeBLAS



object breeze_start extends App {
//  println(NativeBLAS.getInstance())
//  Создание объектов
  println(DenseMatrix.zeros[Double](3, 5))
  println(DenseVector.fill(3){5.0})
  println(DenseMatrix((1.0, 2.0), (3.0, 4.0)))

  println(linspace(2, 7, 4))
  println(DenseMatrix.tabulate(3, 2){case (i, j) => i + j})
  println(DenseMatrix.rand(2, 3))
//  Слайсинг и индексация
  val m = DenseMatrix.tabulate(3, 3){case (i, j) => i - j}
  val cat = m(::, 1 to 2)
  val e = m(-1, 1)
  val v = DenseVector(1.0, 2.0, 3.0)
  val A = DenseMatrix((1.0, 2.0, 3.0), (1.0, 0.0, 2.0))
  println(A * v)
  println(v dot v)
  println(v.t * v)
  println(A * A.t)


//  Поэлементные операции
      val vv = DenseVector(1.0, 2.0, 3.0)
      println(vv *:* vv)
      vv :+= vv
      println(vv)
      println(vv :*= 3.0)
      println(vv <:< (vv +:+ vv))
      println(argmax(vv))
//  Агрегаты
  val AA = DenseMatrix((1.0, 2.0, 3.0), (1.0, 0.0, 2.0))
  println(sum(AA))
  println(mean(AA(::, *)))
  println(trace(AA(::, 0 to 1)))
  println(accumulate(AA.toDenseVector))
//  Линейная алгебра
  val AAA = DenseMatrix((1.0, 2.0), (5.0, 3.0))
  val b = AAA * DenseVector(3.0, 1.0)

  println(AAA \ b)
  println(det(AAA))
  println(inv(AAA))
  println(eig(AAA))


}