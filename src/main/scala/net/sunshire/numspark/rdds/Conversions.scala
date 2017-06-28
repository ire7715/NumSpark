package net.sunshire.numspark.rdds;

import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.{DataFrame, Row, SQLContext};

object Conversions {
  /**
    * Implicitly convert RDD[Row] to DataFrame.
    *
    * @param rdd: RDD[Row]
    * @return DataFrame
    */
  implicit def RDDRow2DataFrame(rdd: RDD[Row]): DataFrame = {
    val sc = rdd.sparkContext;
    val sqlContext = SQLContext.getOrCreate(sc);
    val schema = rdd.first.schema;
    return sqlContext.createDataFrame(rdd, schema);
  }
}
