package net.sunshire.numspark.ml

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.{Pipeline}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.sql.types._
import scala.util.Random

/**
  * Represents a tree node.
  *
  * @param column: StructField; colmun information.
  * @param value: Any; the divide value of this node, left subtree are all elements <= $value. Using Any to match Row.getAs
  * @param left: Option[IsolationTreeNode]; left subtree, could be None. Represents elements <= $value.
  * @param right: Option[IsolationTreeNode]; right subtree, could be None. Represents elements > $value.
  */
private[ml] case class IsolationTreeNode(
    column: StructField,
    value: Any,
    left: Option[IsolationTreeNode],
    right: Option[IsolationTreeNode]) extends java.io.Serializable

private[ml] object IsolationTreeNode extends java.io.Serializable {
  /**
    * Recusive function to compute the path length of the element in the tree.
    *
    * @param row: Row; the row to compute the path length.
    * @param rootOption: Option[IsolationTree]; the tree to traverse.
    * @param depth: Int; represents the current depth, defaults 0. This parameter is used for the internal call only.
    * @return Int
    */
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

/**
  * The trained isolation forest model.
  *
  * @param trees: Seq[IsolationTreeModel]; trees to represents a forest.
  */
class IsolationForestModel(trees: Seq[IsolationTreeModel]) {
  private val treeCount = trees.length

  /**
    * The prediction function.
    *
    * @param testset: DataFrame
    * @return DataFrame: same as the $testset, with the new column "pathLengthAverage". And ordered by it in descending.
    */
  def transform(testset: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions
    val sqlContext = testset.sqlContext

    val allColumns = testset.columns.map(testset(_))
    val pathLengthColumns = for (i <- 0 until treeCount) yield {
      val rootOptionBr = sqlContext.sparkContext.broadcast(trees(i).modelOption)
      val pathLengthUDF = sqlContext.udf.register("pathLength" + i,
        IsolationTreeNode.pathLength(_: Row, rootOptionBr.value, 0))
      pathLengthUDF(functions.struct(allColumns: _*)).as("pathLength" + i)
    }
    val pathLengthsAvg = (pathLengthColumns.reduce(_ + _) / lit(treeCount)).as("pathLengthAverage")
    testset.select((allColumns ++ pathLengthColumns): _*)
    .select((allColumns :+ pathLengthsAvg): _*)
    .orderBy(pathLengthsAvg.desc)
  }

  def printModel {
    // to-do
    println("To be implemented.")
  }
}

/**
  * The entry of isolation forest.
  *
  * @param data: DataFrame; the input data.
  * @param treeCount: Int; the number of trees. Data will be divided evenly to each tree and be trained.
  * @param maxDepth: the max depth of the constructed trees.
  */
class IsolationForest(data: DataFrame, treeCount: Int, maxDepth: Int) {
  /**
    * Train the model.
    *
    * @return IsolationForestModel
    */
  def fit = new IsolationForestModel(
    for (i <- 1 to treeCount)
      yield new IsolationTree(data.sample(false, 1.0 / treeCount), maxDepth).fit
  )
}

/**
  * The trained isolation tree model.
  *
  * @param rootOption: Option[IsolationTreeNode]; the tree structure
  */
class IsolationTreeModel(rootOption: Option[IsolationTreeNode]) {
  private[ml] val modelOption = rootOption

  /**
    * The predict function.
    *
    * @param testset: DataFrame; the data to be predicted.
    * @return DataFrame; the testset with new column named "pathLenth". Ordered by it in descending.
    */
  def transform(testset: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions
    val sqlContext = testset.sqlContext
    val rootOptionBr = sqlContext.sparkContext.broadcast(rootOption)
    val pathLengthUDF = sqlContext.udf.register(
      "pathLength",
      IsolationTreeNode.pathLength(_: Row, rootOptionBr.value, 0))
    val allColumns = testset.columns.map(testset(_))
    val pathLengthColumn = pathLengthUDF(functions.struct(allColumns: _*)).as("pathLength")
    val withPathLength = allColumns :+ pathLengthColumn
    testset.select(withPathLength: _*).orderBy(pathLengthColumn.desc)
  }

  def printModel {
    // to-do
    println("To be implemented.")
  }
}

/**
  * The entry of the tree.
  *
  * @param data: DataFrame; the input data.
  * @param maxDepth: Int; the max depth of the tree to contstruct.
  */
class IsolationTree(data: DataFrame, maxDepth: Int) {
  def fit = new IsolationTreeModel(grow(data))

  /**
    * Choose from a Seq randomly.
    *
    * @param seq: Seq[T]; the sequence to be choose.
    * @return Option[T]; the chosen element, could be None when the sequence has no element.
    */
  private def randomChoice[T](seq: Seq[T]): Option[T] = {
    if (seq.isEmpty) return None
    else Option(seq(Random.nextInt(seq.length)))
  }

  /**
    * Choose the pivot value to divide the DataFrame into two.
    *
    * @param data: DataFrame; the data to be divided.
    * @param field: StructField; the column applied to divide.
    * @param min: Any; the lower bound (inclusive).
    * @param max: Any; the upper bound (exclusive).
    * @return Tuple3[Any, DataFrame, DataFrame]; the pivot value and the divided two DataFrames.
    */
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

  /**
    * Recusive function to grow the tree.
    *
    * @param data: DataFrame; the input data.
    * @param depth: Int; the current depth of the tree, defaults 0. This parameter is used for internal call only
    * @return Option[IsolationTreeNode]
    */
  private def grow(data: DataFrame, depth: Int = 0): Option[IsolationTreeNode] = {
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

  /**
    * Compute each columnn's boundaries.
    *
    * @param data: DataFrame; the input data.
    * @return Seq[Tuple3[StructField, Any, Any]]; a sequence of each column and its min/max value.
    */
  private def columnAndBoundary(data: DataFrame): Seq[(StructField, Any, Any)] = {
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