package net.sunshire.numspark.dataframes.linalg;

import org.apache.spark.sql.{Column, DataFrame, SQLContext};

object matrix {
  def dot(
    sqlContext: SQLContext,
    m1: DataFrame,
    m2: DataFrame,
    m1ijk: Tuple3[String, String, String],
    m2ijk: Tuple3[String, String, String]
  ): DataFrame = {
    import sqlContext.implicits._;
    import org.apache.spark.sql.functions._;

    val asM1 = m1.as("m1");
    val asM2 = m2.as("m2");
    val joinCondition = col("m1." + m1ijk._2) === col("m2." + m2ijk._1);
    val newRowIndex = col("m1." + m1ijk._1).as("row");
    val newColIndex = col("m2." + m2ijk._2).as("col");
    val m1k = col("m1." + m1ijk._3);
    val m2k = col("m2." + m2ijk._3);
    val dots = asM1.join(asM2, joinCondition)
      .groupBy(newRowIndex, newColIndex)
      .agg(sum(m1k * m2k).as("dot"));
    return dots;
  }

  def norm(
    n: Int,
    sqlContext: SQLContext,
    m: DataFrame,
    by: String,
    value: String
  ): DataFrame = {
    if(n < 1)
      throw new Exception(n + "-norm is not a valid expression");
    import sqlContext.implicits._;
    import org.apache.spark.sql.functions._;

    var normProduct = col(value);
    for (i <- 1 until n)
      normProduct = normProduct * col(value);
    val normFormula =
      if(n > 1) pow(sum(normProduct), 1.0 / n)
      else abs(sum(normProduct));
    val norms = m.groupBy(col(by)).agg(normFormula.as("norm"));
    return norms;
  }
}
