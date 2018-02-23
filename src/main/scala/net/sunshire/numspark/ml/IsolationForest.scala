package net.sunshire.numspark.ml

import java.util.NoSuchElementException
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.{Pipeline}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, SQLContext}
import org.apache.spark.sql.types._
import net.sunshire.numspark.utils.ExtendedRandom._
import scala.math
import scala.util.Random

/**
  * Represents a tree node. To represent a external node, assign every attribute to `null` except size.
  *
  * @param column: colmun information.
  * @param value: the divide value of this node, left subtree are all elements <= $value. Using Any to match Row.getAs
  * @param size: the number of training instances contained in this subtree.
  * @param left: left subtree, could be None. Represents elements <= $value.
  * @param right: right subtree, could be None. Represents elements > $value.
  */
private[ml] case class IsolationTreeNode(
    column: StructField,
    value: Any,
    size: Long,
    left: IsolationTreeNode,
    right: IsolationTreeNode) extends java.io.Serializable {

  /**
    * to test if a node is an external node.
    */
  lazy val isExternal = (value == null)

  /**
    * flatten the tree, transform it into a Seq
    *
    * @param treeId: the id of which the root belongs to
    * @param index: the index of this tree
    * @return NodeData and its index sequence
    */
  private[ml] def toSeq(treeId: Int, index: Int): Seq[IsolationTreeNodeData] = {
    val leftId = if (left == null) 0 else index * 2
    val rightId = if (right == null) 0 else index * 2 + 1
    val root = Seq(IsolationTreeNodeData(treeId, index, IsolationTreeNodeData.column2tuple(column),
      value, size, leftId, rightId))
    val leftSeq = if (leftId == 0) Nil else left.toSeq(treeId, leftId)
    val rightSeq = if (rightId == 0) Nil else right.toSeq(treeId, rightId)
    root ++
    leftSeq.asInstanceOf[Seq[IsolationTreeNodeData]] ++
    rightSeq.asInstanceOf[Seq[IsolationTreeNodeData]]
  }

  /**
    * flatten the tree, transform it into a Seq
    *
    * @param treeId: the id of which the root belongs to
    * @return NodeData and its index sequence
    */
  def toSeq(treeId: Int): Seq[IsolationTreeNodeData] = toSeq(treeId, 1)
}

/**
  * NodeData represents flatten tree, for storage usage.
  *
  * @param treeId: the id of this tree
  * @param index: the node index in the tree, root is 1, left-child is {{index * 2}}, right-child is {{index * 2 + 1}}
  * @param column: represents column using Tuple4, [Name, DataType, Nullable, Metadata] respetively. DataType and Metadata applies json to reconstruct.
  * @param value
  * @param size
  * @param leftId: the index of the left child
  * @param rightId: the index of the right chlid
  */
private[ml] case class IsolationTreeNodeData(
    treeId: Int,
    index: Int,
    column: (String, String, Boolean, String),
    value: Any,
    size: Long,
    leftId: Int,
    rightId: Int) extends java.io.Serializable {
  /**
    * test this node is external node
    */
  lazy val isExternal = leftId == 0 && rightId == 0
}

private[ml] object IsolationTreeNodeData extends java.io.Serializable {
  /**
    * transform StructField to Tuple4
    *
    * @param column: the StructField to be transformed
    * @return each field represents a value of StructField. Name, DataType, Nullable, and Metadata respectively.
    */
  def column2tuple(column: StructField): (String, String, Boolean, String) = {
    if (column == null) null
    else (column.name, column.dataType.json, column.nullable, column.metadata.json)
  }

  /**
    * reverse of column2tuple
    *
    * @param tuple
    */
  def tuple2column(tuple: (String, String, Boolean, String)): StructField = {
    if (tuple == null) null
    else StructField(tuple._1, DataType.fromJson(tuple._2), tuple._3, Metadata.fromJson(tuple._4))
  }
}

private[ml] object IsolationTreeExternalNode {
  /**
    * Create an external node.
    *
    * @param size: the number of training instances included in this node.
    */
  def apply(size: Long): IsolationTreeNode = new IsolationTreeNode(null, null, size, null, null)
}

private[ml] object IsolationTreeNode extends java.io.Serializable {
  /**
    * Recusive function to compute the path length of the element in the tree.
    *
    * @param row: the row to compute the path length.
    * @param root: the tree to traverse.
    * @param depth: represents the current depth, defaults 0. This parameter is used for the internal call only.
    * @return the estimated depth.
    */
  def pathLength(row: Row, root: IsolationTreeNode, depth: Double = 0.0): Double = {
    if (root.isExternal) {
      depth + (if (root.size > 1) IsolationTreeNode.adjustment(root.size) else 0)
    } else {
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

  /**
    * The adjustment function for estimating the path length of an external node.
    *
    * @param treeSize: the sample size of this subtree.
    * @return 2 * harmonic(treeSize - 1) - 2 * (treeSize - 1) / treeSize
    */
  def adjustment(treeSize: Long): Double = 2 * harmonic(treeSize - 1) - 2 * (treeSize - 1) / treeSize

  /**
    * The harmonic number, ln(i) + Euler's constant
    *
    * @param i
    * @return ln(i) + 0.5772156649(Euler's constant)
    */
  def harmonic(i: Long): Double = math.log(i) + 0.5772156649

  /**
    * The anomaly score.
    *
    * @param avgPathLength: the E(h(x)) in the paper, represents the mean path length of a instance to a forest
    * @param treeSize: the subsampling size of the training set.
    * @return the anomaly score
    */
  def anomalyScore(avgPathLength: Double, treeSize: Long): Double = math.pow(2,
    -avgPathLength / adjustment(treeSize))
}

/**
  * The trained isolation forest model.
  *
  * @param trees: trees to represents a forest.
  */
class IsolationForestModel(trees: Seq[IsolationTreeModel]) {
  private[ml] val treeModels = trees
  private[ml] val treeCount = trees.length

  /**
    * The prediction function.
    *
    * @param testset:
    * @return same as the $testset, with the new column "anomalyScore". And ordered by it in descending.
    */
  def transform(testset: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions
    val sqlContext = testset.sqlContext

    val allColumns = testset.columns.map(testset(_))
    val pathLengthColumns = for (i <- 0 until treeCount) yield {
      val rootBr = sqlContext.sparkContext.broadcast(trees(i).model)
      val pathLengthUDF = sqlContext.udf.register("pathLength" + i,
        IsolationTreeNode.pathLength(_: Row, rootBr.value))
      pathLengthUDF(functions.struct(allColumns: _*)).as("pathLength" + i)
    }
    val treeSizeBr = sqlContext.sparkContext.broadcast(trees(0).model.size)
    val anomalyScoreUDF = sqlContext.udf.register("anomalyScore",
      IsolationTreeNode.anomalyScore(_: Double, treeSizeBr.value))
    val anomalyScore = (
      anomalyScoreUDF(pathLengthColumns.reduce(_ + _) / functions.lit(treeCount))
    ).as("anomalyScore")
    testset.select((allColumns ++ pathLengthColumns): _*)
    .select((allColumns :+ anomalyScore): _*)
  }

  /**
    * Predict a the anomaly by given threshold.
    *
    * @param testset: the dataset to be predicted
    * @param threshold: the threshold to be predict, which should between 0 and 1.
    * @return the origin dataframe with a new column "isOulier"
    */
  def predict(testset: DataFrame, threshold: Double = 0.6): DataFrame = {
    import org.apache.spark.sql.functions._
    transform(testset).withColumn("isOutlier", col("anomalyScore") >= lit(threshold))
    .drop("anomalyScore")
  }

  /**
    * Write the forest to the file system.
    *
    * @param path: the path to save the model.
    */
  def write(path: String) {
    import java.io._
    val oos = new ObjectOutputStream(new FileOutputStream(path))
    oos.writeObject(trees.map(_.model))
    oos.close
  }

  def printModel {
    // to-do
    println("To be implemented.")
  }
}

object IsolationForest {
  /**
    * Read the model from file system.
    *
    * @param path: the path to the existent model file.
    * @return the forest model
    */
  def readModel(path: String): IsolationForestModel = {
    import java.io._
    val ois = new ObjectInputStream(new FileInputStream(path))
    val roots = ois.readObject.asInstanceOf[Seq[IsolationTreeNode]]
    ois.close
    new IsolationForestModel(roots.map(new IsolationTreeModel(_)))
  }
}

/**
  * The entry of isolation forest.
  *
  * @param data: the input data.
  * @param treeCount: the number of trees. Data will be divided evenly to each tree and be trained.
  * @param samplingSize: size for training.
  */
class IsolationForest(data: DataFrame, treeCount: Int, samplingSize: Int) {
  val size = data.count
  val samplingFraction = samplingSize.toDouble / size
  val maxDepth = math.ceil(math.log(samplingSize) / math.log(2)).toInt
  val withReplacement = samplingFraction > (1.0 / treeCount)

  /**
    * Train the model.
    *
    * @return IsolationForestModel
    */
  def fit: IsolationForestModel = {
    def getTree = {
      var tree = new IsolationTree(data.sample(withReplacement, samplingFraction), maxDepth).fit
      while (tree.model.isExternal) {
        tree = new IsolationTree(data.sample(withReplacement, samplingFraction), maxDepth).fit
      }
      tree
    }
    new IsolationForestModel(for (i <- 1 to treeCount) yield getTree)
  }
}

/**
  * The trained isolation tree model.
  *
  * @param root: the tree structure
  */
class IsolationTreeModel(root: IsolationTreeNode) {
  private[ml] val model = root

  /**
    * The predict function.
    *
    * @param testset: the data to be predicted.
    * @return the testset with new column named "pathLenth". Ordered by it in descending.
    */
  def transform(testset: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions
    val sqlContext = testset.sqlContext
    val rootBr = sqlContext.sparkContext.broadcast(root)
    val pathLengthUDF = sqlContext.udf.register(
      "pathLength",
      IsolationTreeNode.pathLength(_: Row, rootBr.value, 0))
    val allColumns = testset.columns.map(testset(_))
    val pathLengthColumn = pathLengthUDF(functions.struct(allColumns: _*)).as("pathLength")
    val withPathLength = allColumns :+ pathLengthColumn
    testset.select(withPathLength: _*).orderBy(pathLengthColumn.desc)
  }

  /**
    * save the model to file system.
    *
    * @param sc: the [[SparkContext]] which is going to be applied
    * @param path: the path for the model storage.
    * @param treeId: the tree id
    */
  def save(sc: SparkContext, path: String, treeId: Int = 0) {
    val rootData = root.toSeq(treeId)
    val dataRDD = sc.parallelize(rootData)
    dataRDD.saveAsObjectFile(path)
  }

  def printModel {
    // to-do
    println("To be implemented.")
  }
}

object IsolationTree {
  /**
    * Compute each columnn's boundaries.
    *
    * @param data: the input data.
    * @return a sequence of each column and its min/max value.
    */
  private[ml] def columnAndBoundary(data: DataFrame): Seq[(StructField, Any, Any)] = {
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

  /**
    * Choose the pivot value to divide the DataFrame into two.
    *
    * @param data: the data to be divided.
    * @param field: the column applied to divide.
    * @param min: the lower bound (inclusive).
    * @param max: the upper bound (exclusive).
    * @return the pivot value and the divided two DataFrames.
    */
  private[ml] def randomPivot(
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
    * Read the model from file system.
    *
    * @param path: the path to the existent model file.
    * @return the tree model
    */
  def load(sc: SparkContext, path: String): IsolationTreeModel = {
    val dataRDD = sc.objectFile[IsolationTreeNodeData](path)
    new IsolationTreeModel(constructTree(dataRDD.collect))
  }

  /**
    * Construct tree from sequence of node data.
    *
    * @param nodes
    */
  private[ml] def constructTree(nodes: Seq[IsolationTreeNodeData]): IsolationTreeNode = {
    val dataMap = nodes.map(node => node.index -> node).toMap
    constructNode(1, dataMap)
  }

  /**
    * from node data map to build a tree
    *
    * @param index: curent node index
    * @param dataMap: the node data map
    */
  private[ml] def constructNode(
      index: Int,
      dataMap: Map[Int, IsolationTreeNodeData]): IsolationTreeNode = {
    val nodeData = dataMap(index)
    if (nodeData.isExternal) IsolationTreeExternalNode(nodeData.size)
    else new IsolationTreeNode(IsolationTreeNodeData.tuple2column(nodeData.column),
      nodeData.value, nodeData.size,
      constructNode(index * 2, dataMap), constructNode(index * 2 + 1, dataMap))
  }
}

/**
  * The entry of the tree.
  *
  * @param data: the input data.
  * @param maxDepth: the max depth of the tree to contstruct.
  */
class IsolationTree(data: DataFrame, maxDepth: Int) {
  def fit = new IsolationTreeModel(grow(data))

  /**
    * Recusive function to grow the tree.
    *
    * @param data: the input data.
    * @param depth: the current depth of the tree, defaults 0. This parameter is used for internal call only
    * @return
    */
  private def grow(data: DataFrame, depth: Int = 0): IsolationTreeNode = {
    if (depth >= maxDepth) return IsolationTreeExternalNode(data.count)
    val columns = IsolationTree.columnAndBoundary(data)
    .filter{ case (column, min, max) => min != max }
    val choosedColumn = Random.choice(columns)
    if (choosedColumn.isEmpty) return IsolationTreeExternalNode(data.count)
    val (column, min, max) = choosedColumn.get
    val pivot = IsolationTree.randomPivot(data, column, min, max)
    val filteredColumns = columns.map(field => data.col(field._1.name))
    return IsolationTreeNode(
      column,
      pivot._1,
      data.count,
      grow(pivot._2.select(filteredColumns: _*), depth + 1),
      grow(pivot._3.select(filteredColumns: _*), depth + 1)
    )
  }
}
