package net.sunshire.numspark.ml

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.math

class IsolationForestSuite extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var sqlContext: SQLContext = _
  var schema: StructType = _
  var data: DataFrame = _
  before {
    sqlContext = new SQLContext(sc)
    schema = StructType(Seq(
      StructField("bool", BooleanType),
      StructField("byte", ByteType),
      StructField("double", DoubleType),
      StructField("float", FloatType),
      StructField("int", IntegerType),
      StructField("long", LongType),
      StructField("short", ShortType)
    ))
    data = sqlContext.createDataFrame(sc.parallelize(Seq(
      Row(false, 1.toByte, 1.0, (1.0).toFloat, 1, 1.toLong, 1.toShort),
      Row(true, 2.toByte, 2.0, (2.0).toFloat, 2, 2.toLong, 2.toShort)
    )), schema)
  }

  test("IsolationTree.columnAndBoundary") {
    val expected = Map(
      "bool" -> (false, true),
      "byte" -> (1.toByte, 2.toByte),
      "double" -> (1.0, 2.0),
      "float" -> ((1.0).toFloat, (2.0).toFloat),
      "int" -> (1, 2),
      "long" -> (1.toLong, 2.toLong),
      "short" -> (1.toShort, 2.toShort)
    )
    val results: Seq[(StructField, Any, Any)] = IsolationTree.columnAndBoundary(data)
    for (result <- results) {
      assert(result._2 == expected(result._1.name)._1)
      assert(result._3 == expected(result._1.name)._2)
    }
  }

  test("IsolationTree.randomPivot") {
    import org.apache.spark.sql.functions
    val expectedRange = Map(
      "bool" -> (false, true),
      "byte" -> (1.toByte, 2.toByte),
      "double" -> (1.0, 2.0),
      "float" -> ((1.0).toFloat, (2.0).toFloat),
      "int" -> (1, 2),
      "long" -> (1.toLong, 2.toLong),
      "short" -> (1.toShort, 2.toShort)
    )
    val schema = data.schema
    for ((name, values) <- expectedRange) {
      val column = schema.apply(name)
      val (pivot, left, right): (Any, DataFrame, DataFrame) = IsolationTree.randomPivot(
        data, column, values._1, values._2)
      val min = left.select(functions.max(left.col(name))).first.apply(0)
      val max = right.select(functions.min(right.col(name))).first.apply(0)
      column.dataType match {
        case BooleanType =>
          assert(pivot.asInstanceOf[Boolean] == false)
          assert(min.asInstanceOf[Boolean] == false)
          assert(max.asInstanceOf[Boolean] == true)
        case ByteType =>
          assert(values._1.asInstanceOf[Byte] <= pivot.asInstanceOf[Byte] &&
            pivot.asInstanceOf[Byte] < values._2.asInstanceOf[Byte])
          assert(values._1.asInstanceOf[Byte] <= min.asInstanceOf[Byte] &&
            min.asInstanceOf[Byte] <= pivot.asInstanceOf[Byte])
          assert(pivot.asInstanceOf[Byte] < max.asInstanceOf[Byte] &&
            max.asInstanceOf[Byte] <= values._2.asInstanceOf[Byte])
        // case DateType =>
        // to-do
        // case DecimalType =>
        // to-do
        case DoubleType =>
          assert(values._1.asInstanceOf[Double] <= pivot.asInstanceOf[Double] &&
            pivot.asInstanceOf[Double] < values._2.asInstanceOf[Double])
          assert(values._1.asInstanceOf[Double] <= min.asInstanceOf[Double] &&
            min.asInstanceOf[Double] <= pivot.asInstanceOf[Double])
          assert(pivot.asInstanceOf[Double] < max.asInstanceOf[Double] &&
            max.asInstanceOf[Double] <= values._2.asInstanceOf[Double])
        case FloatType =>
          assert(values._1.asInstanceOf[Float] <= pivot.asInstanceOf[Float] &&
            pivot.asInstanceOf[Float] < values._2.asInstanceOf[Float])
          assert(values._1.asInstanceOf[Float] <= min.asInstanceOf[Float] &&
            min.asInstanceOf[Float] <= pivot.asInstanceOf[Float])
          assert(pivot.asInstanceOf[Float] < max.asInstanceOf[Float] &&
            max.asInstanceOf[Float] <= values._2.asInstanceOf[Float])
        case IntegerType =>
          assert(values._1.asInstanceOf[Integer] <= pivot.asInstanceOf[Integer] &&
            pivot.asInstanceOf[Integer] < values._2.asInstanceOf[Integer])
          assert(values._1.asInstanceOf[Integer] <= min.asInstanceOf[Integer] &&
            min.asInstanceOf[Integer] <= pivot.asInstanceOf[Integer])
          assert(pivot.asInstanceOf[Integer] < max.asInstanceOf[Integer] &&
            max.asInstanceOf[Integer] <= values._2.asInstanceOf[Integer])
        case LongType =>
          assert(values._1.asInstanceOf[Long] <= pivot.asInstanceOf[Long] &&
            pivot.asInstanceOf[Long] < values._2.asInstanceOf[Long])
          assert(values._1.asInstanceOf[Long] <= min.asInstanceOf[Long] &&
            min.asInstanceOf[Long] <= pivot.asInstanceOf[Long])
          assert(pivot.asInstanceOf[Long] < max.asInstanceOf[Long] &&
            max.asInstanceOf[Long] <= values._2.asInstanceOf[Long])
        case ShortType =>
          assert(values._1.asInstanceOf[Short] <= pivot.asInstanceOf[Short] &&
            pivot.asInstanceOf[Short] < values._2.asInstanceOf[Short])
          assert(values._1.asInstanceOf[Short] <= min.asInstanceOf[Short] &&
            min.asInstanceOf[Short] <= pivot.asInstanceOf[Short])
          assert(pivot.asInstanceOf[Short] < max.asInstanceOf[Short] &&
            max.asInstanceOf[Short] <= values._2.asInstanceOf[Short])
      }
    }
  }
}
