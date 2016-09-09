package net.sunshire.numspark.dataframes.linalg;

import org.apache.spark.sql.{Column, DataFrame, SQLContext};

object vectors {
  def dot(
    sqlContext: SQLContext,
    vs1: DataFrame,
    vs2: DataFrame,
    vs1ijk: Tuple3[String, String, String],
    vs2ijk: Tuple3[String, String, String]
  ): DataFrame = {
    import sqlContext.implicits._;
    import org.apache.spark.sql.functions._;

    val asVs1 = vs1.as("vs1");
    val asVs2 = vs2.as("vs2");
    val joinCondition = col("vs1." + vs1ijk._2) === col("vs2." + vs2ijk._2);
    val newRowIndex = col("vs1." + vs1ijk._1).as("row");
    val newColIndex = col("vs2." + vs2ijk._1).as("col");
    val vs1k = col("vs1." + vs1ijk._3);
    val vs2k = col("vs2." + vs2ijk._3);
    val dots = asVs1.join(asVs2, joinCondition)
      .groupBy(newRowIndex, newColIndex)
      .agg(sum(vs1k * vs2k).as("dot"));
    return dots;
  }

  def norm(
    n: Int,
    sqlContext: SQLContext,
    vs: DataFrame,
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
    val norms = vs.groupBy(col(by)).agg(normFormula.as("norm"));
    return norms;
  }
}
