package org.apache.spark.ml.otus

import breeze.linalg
import breeze.linalg.{DenseVector, HashVector}
import breeze.numerics.sqrt
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{IntParam, Param, ParamMap}
import org.apache.spark.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, functions}

/**
  * Holds the parameters shared by both model and the estimator
  */
trait KnnRecommenderParams extends HasInputCol with HasOutputCol {

  // TODO : Support top-K
  val topK = new IntParam(this, "topK",
    "Number of similar items to report.",
    (x: Int) => x > 0)

  def setOutputCol(value: String) : this.type = set(outputCol, value)
  def setInputCol(value: String) : this.type =set(inputCol, value)

  private[otus] def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, getInputCol, new VectorUDT())

    if (schema.contains($(outputCol))) {
      SchemaUtils.checkColumnType(schema, getOutputCol, new VectorUDT())
      schema
    } else {
      SchemaUtils.appendColumn(schema, schema(getInputCol).copy(name = getOutputCol))
    }
  }
}

/**
  * Utility class used to pass similarity info from estimator to model
  */
// TODO: Use spark CSCMatrix instead of Dataset (construct through breeze)
private[otus] case class SimilarityRecord(id: Int, topSimilars: Vector)

class KnnRecommenderModel(
                           override val uid: String,
                           private[otus] val similarity: Dataset[SimilarityRecord]
                         )
  extends Model[KnnRecommenderModel] with KnnRecommenderParams with MLWritable {

  private[otus] def this(uid : String) = this(uid, null)

  def this(similarity: Dataset[SimilarityRecord]) = this(
    Identifiable.randomUID("knn"), similarity)

  override def copy(extra: ParamMap): KnnRecommenderModel =
    copyValues(new KnnRecommenderModel(similarity), extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // TODO: Replace with broadcast join
    val broadcast = dataset.sparkSession.sparkContext.broadcast(similarity.collect().sortBy(_.id))

    val recommend = functions.udf((x: Vector) => {
      val sum = HashVector.zeros[Double](x.size)
      x.foreachActive((i, v) =>
        broadcast.value(i).topSimilars.foreachActive((j, sim) => sum(j) += v * sim))

      Vectors.sparse(x.size, sum.activeIterator.toSeq).compressed
    })

    dataset.withColumn($(outputCol), recommend(dataset($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType =
    validateAndTransformSchema(schema)

  override def write: MLWriter = new DefaultParamsWriter(this) {
    protected override def saveImpl(path: String): Unit = {
      super.saveImpl(path)
      similarity.write.parquet(path + "/similarity")
    }
  }
}


object KnnRecommenderModel extends MLReadable[KnnRecommenderModel]{
  override def read: MLReader[KnnRecommenderModel] = new MLReader[KnnRecommenderModel] {
    override def load(path: String): KnnRecommenderModel = {
      val sqlc = sqlContext

      import sqlc.implicits._

      val original = new DefaultParamsReader().load(path).asInstanceOf[KnnRecommenderModel]
      val similarity = sqlContext.read.parquet(path + "/similarity").as[SimilarityRecord]

      original.copyValues(new KnnRecommenderModel(similarity))
    }
  }
}


class KnnRecommender(override val uid: String) extends
  Estimator[KnnRecommenderModel] with KnnRecommenderParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("knnEstimator"))

  override def fit(dataset: Dataset[_]): KnnRecommenderModel = {
    import dataset.sqlContext.implicits._

    // Used to convert untyped dataframes to datasets with vectors
    implicit val encoder : Encoder[Vector] = ExpressionEncoder()

    // Find the amount of items from metadata
    val numItems = AttributeGroup.fromStructField(dataset.schema($(inputCol)))
      .numAttributes.getOrElse(Int.MaxValue)

    // Compute item vector L2 norms
    val norms: linalg.Vector[Double] = sqrt(dataset
      .select(dataset($(inputCol))).as[Vector].rdd.mapPartitions(iter => {
      // TODO: Choose between Dense and hash vector based on numItems and param
      val result: linalg.Vector[Double] = DenseVector.zeros[Double](numItems)

      iter.foreach(_.foreachActive((i, v) => result(i) += v * v))

      Iterator(result)
    }).reduce(_ + _))

    // Compute item-item dot products and divide by product of norms (cosine)
    val triangleSimilarity: Dataset[((Int, Int), Double)] = dataset
      .select(dataset($(inputCol))).as[Vector]
      .flatMap(r => {
        val vec = r.asBreeze

        vec.activeIterator.flatMap {
          case (curItem, curRating) => vec.activeIterator.filter(_._1 > curItem).map {
            case (otherItem, otherRating) => (curItem, otherItem) -> curRating * otherRating
          }
        }
      })
      // TODO: Configure number of partitions
      .groupByKey(_._1).mapValues(_._2).reduceGroups(_ + _)
      // TODO: Replace with broadcast join
      .map(x => x._1 -> x._2 / (norms(x._1._1) * norms(x._1._2)))

    val similarityMatrix: Dataset[((Int, Int), Double)] =
      triangleSimilarity.union(triangleSimilarity.map(x => x._1.swap -> x._2))

    val records: RDD[(Int, HashVector[Double])] = similarityMatrix.rdd.map {
      case (pair, sim) =>
        pair._1 -> {
          // TODO: Use breeze TopK to support topK parameter
          val similars = HashVector.zeros[Double](numItems)
          similars(pair._2) = sim
          similars
        }
    }
      // TODO: Configure number of partitions
      .reduceByKey(_ + _)

    // Convert to dataset
    val similarity: Dataset[SimilarityRecord] = records.map {
      case (user, similars) => SimilarityRecord(
        user, Vectors.sparse(numItems, similars.activeIterator.toSeq).compressed)
    }.toDS()

    // Construct result
    copyValues(new KnnRecommenderModel(similarity)).setParent(this)
  }

  override def copy(extra: ParamMap): Estimator[KnnRecommenderModel] =
    defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

object KnnRecommender extends DefaultParamsReadable[KnnRecommender]
