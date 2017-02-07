package net.sunshire.numspark.dataframes.linalg;

import org.apache.spark.sql.{Column, DataFrame, SQLContext};

object matrix {
  def dot(
    mat1: (DataFrame, (String, String, String)),
    mat2: (DataFrame, (String, String, String))
  ): DataFrame = {
    val (m1, m1ijk) = mat1;
    val (m2, m2ijk) = mat2;

    val sqlContext = m1.sqlContext;
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
    m: DataFrame,
    by: String,
    value: String
  ): DataFrame = {
    if(n < 1)
      throw new Exception(n + "-norm is not a valid expression");

    val sqlContext = m.sqlContext;
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
