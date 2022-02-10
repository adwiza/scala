
import java.io.File

import breeze.linalg.{*, DenseMatrix, DenseVector, diag, max, normalize}
import org.apache.commons.io.FileUtils
import org.apache.spark.ml
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

import org.scala.collection.immutable
import org.scala.util

