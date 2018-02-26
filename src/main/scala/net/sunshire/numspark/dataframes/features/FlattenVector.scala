package net.sunshire.numspark.dataframes.features

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FlattenVector {
  def apply(
      dataframe: DataFrame, flattenColumns: Map[String, String], preserveOld: Boolean = false
  ): DataFrame = {
    val sqlContext = dataframe.sqlContext
    val getItemUDF = sqlContext.udf.register("getVectorItem",
      (vector: org.apache.spark.ml.linalg.Vector, i: Int) => vector.apply(i))
    val allColumnsName = if (preserveOld) dataframe.columns
      else dataframe.columns.filterNot(flattenColumns.keySet.contains(_))
    val allColumns = allColumnsName.map(dataframe(_))
    val first = dataframe.first
    val outputColumns = flattenColumns.flatMap{ case (input, output) =>
      val size = first.getAs[Vector](input).size
      (0 until size).map(i => getItemUDF(col(input), lit(i)).as(s"${output}_${i}"))
    }
    dataframe.select((allColumns ++ outputColumns): _*)
  }
}
