package net.sunshire.numspark.rdds;

import com.holdenkarau.spark.testing.SharedSparkContext;
import net.sunshire.numspark.rdds.Conversions._;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.{DataFrame, Row};
import org.scalatest.FunSuite;

class ConversionsSuite extends FunSuite with SharedSparkContext {
  test("implicit type casting(RDD[Row] -> DataFrame)") {
    def hasImplicit(rdd: RDD[Row])
        (implicit conversion: RDD[Row] => DataFrame = null): Boolean = {
      return conversion != null;
    }
    val rdd = sc.emptyRDD[Row];

    assert(hasImplicit(rdd));
  }
}
