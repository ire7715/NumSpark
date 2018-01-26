package net.sunshire.numspark.ml

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.{Pipeline}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.types._
import scala.util.Random

private[ml] case class IsolationTreeNode(
    column: StructField,
    value: Any,
    left: Option[IsolationTreeNode],
    right: Option[IsolationTreeNode]) extends java.io.Serializable

private[ml] object IsolationTreeNode extends java.io.Serializable {
  def pathLength(row: Row, rootOption: Option[IsolationTreeNode], depth: Int = 0): Int = {
    if (rootOption.isEmpty) depth
    else {
      val root = rootOption.get
      val column = root.column
      column.dataType match {
        case BooleanType =>
          if (row.getAs[Boolean](column.name) == false) pathLength(row, root.left, depth + 1)
          else pathLength(row, root.right, depth + 1)
        case ByteType =>
          if (row.getAs[Byte](column.name) <= root.value.asInstanceOf[Byte]){
            pathLength(row, root.left, depth + 1)
          } else pathLength(row, root.right, depth + 1)
        // case DateType =>
        // to-do
        // case DecimalType =>
        // to-do
        case DoubleType =>
          if (row.getAs[Double](column.name) <= root.value.asInstanceOf[Double]) {
            pathLength(row, root.left, depth + 1)
          } else pathLength(row, root.right, depth + 1)
        case FloatType =>
          if (row.getAs[Float](column.name) <= root.value.asInstanceOf[Float]) {
            pathLength(row, root.left, depth + 1)
          } else pathLength(row, root.right, depth + 1)
        case IntegerType =>
          if (row.getAs[Int](column.name) <= root.value.asInstanceOf[Int]) {
            pathLength(row, root.left, depth + 1)
          } else pathLength(row, root.right, depth + 1)
        case LongType =>
          if (row.getAs[Long](column.name) <= root.value.asInstanceOf[Long]) {
            pathLength(row, root.left, depth + 1)
          } else pathLength(row, root.right, depth + 1)
        case ShortType =>
          if (row.getAs[Short](column.name) <= root.value.asInstanceOf[Short]) {
            pathLength(row, root.left, depth + 1)
          } else pathLength(row, root.right, depth + 1)
      }
    }
  }
}

private[ml] class IsolationTree(data: DataFrame, maxDepth: Int) {
  private lazy val rootOption = grow(data, 0)

  def fit {
    rootOption
  }

  def transform(testset: DataFrame, topK: Int): Array[Row] = {
    val sqlContext = testset.sqlContext
    val rootOptionBr = sqlContext.sparkContext.broadcast(rootOption)
    testset.rdd
    .map(row => (IsolationTreeNode.pathLength(row, rootOptionBr.value), row))
    .top(topK)(Ordering[Int].on(_._1))
    .map(_._2)
  }

  private def randomChoice[T](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty) return None
    else Option(seq(Random.nextInt(seq.length)))
  }

  private def randomPivot(
      data: DataFrame, field: StructField, min: Any, max: Any
  ): (Any, DataFrame, DataFrame) = {
    val column = data.col(field.name)
    val pivot: Any = field.dataType match {
      case BooleanType =>
        false
      case ByteType =>
        val maxByte = max.asInstanceOf[Byte]
        val minByte = min.asInstanceOf[Byte]
        (Random.nextInt(maxByte - minByte) + minByte).toByte
      // case DateType =>
      //   0 // to-do
      // case DecimalType =>
      //   0 // to-do
      case DoubleType =>
        val maxDouble = max.asInstanceOf[Double]
        val minDouble = min.asInstanceOf[Double]
        Random.nextDouble * (maxDouble - minDouble) + minDouble
      case FloatType =>
        val maxFloat = max.asInstanceOf[Float]
        val minFloat = min.asInstanceOf[Float]
        Random.nextFloat * (maxFloat - minFloat) + minFloat
      case IntegerType =>
        val maxInt = max.asInstanceOf[Int]
        val minInt = min.asInstanceOf[Int]
        Random.nextInt(maxInt - minInt) + minInt
      case LongType =>
        val maxLong = max.asInstanceOf[Long]
        val minLong = min.asInstanceOf[Long]
        Random.nextLong % (maxLong - minLong) + minLong
      case ShortType =>
        val maxShort = max.asInstanceOf[Short]
        val minShort = min.asInstanceOf[Short]
        (Random.nextInt(maxShort - minShort) + minShort).toShort
      case _ => throw new Exception(field.name + " is not a numerical column.")
    }
    (pivot, data.filter(column <= pivot), data.filter(column > pivot))
  }

  private def grow(data: DataFrame, depth: Int): Option[IsolationTreeNode] = {
    if (depth >= maxDepth) return None
    val columns = columnAndBoundary(data).filter{case (column, min, max) => min != max}
    val choosedColumn = randomChoice(columns)
    if (choosedColumn.isEmpty) return None
    val (column, min, max) = choosedColumn.get
    val pivot = randomPivot(data, column, min, max)
    val filteredColumns = columns.map(field => data.col(field._1.name))
    return Option(IsolationTreeNode(
      column,
      pivot._1,
      grow(pivot._2.select(filteredColumns: _*), depth + 1),
      grow(pivot._3.select(filteredColumns: _*), depth + 1)
    ))
  }

  def columnAndBoundary(data: DataFrame): Seq[(StructField, Any, Any)] = {
    val columns = data.schema.toList
    val minmaxColumns = columns.map{ _column =>
      import org.apache.spark.sql.functions._
      val name = _column.name
      List(min(name).as(name + "Min"), max(name).as(name + "Max"))
    }.flatten
    val row = data.select(minmaxColumns: _*).first
    (for (column <- columns) yield {
      column.dataType match {
        case BooleanType =>
          (column,
            row.getAs[Boolean](column.name + "Min"),
            row.getAs[Boolean](column.name + "Max"))
        case ByteType =>
          (column,
            row.getAs[Byte](column.name + "Min"),
            row.getAs[Byte](column.name + "Max"))
        // case DateType =>
        //   (column,
        //     row.getAs[java.sql.Date](column.name + "Min"),
        //     row.getAs[java.sql.Date](column.name + "Max"))
        // case DecimalType =>
        //   (column,
        //     row.getAs[Decimal](column.name + "Min"),
        //     row.getAs[Decimal](column.name + "Max"))
        case DoubleType =>
          (column,
            row.getAs[Double](column.name + "Min"),
            row.getAs[Double](column.name + "Max"))
        case FloatType =>
          (column,
            row.getAs[Float](column.name + "Min"),
            row.getAs[Float](column.name + "Max"))
        case IntegerType =>
          (column,
            row.getAs[Int](column.name + "Min"),
            row.getAs[Int](column.name + "Max"))
        case LongType =>
          (column,
            row.getAs[Long](column.name + "Min"),
            row.getAs[Long](column.name + "Max"))
        case ShortType =>
          (column,
            row.getAs[Short](column.name + "Min"),
            row.getAs[Short](column.name + "Max"))
        case _ => throw new Exception(column.name + " is not a numerical column.")
      }
    })
  }
}
