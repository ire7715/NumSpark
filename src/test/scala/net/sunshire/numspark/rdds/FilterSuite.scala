package net.sunshire.numspark.rdds;

import net.sunshire.numspark.rdds.Filter._;
import com.holdenkarau.spark.testing.SharedSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.{Row, SQLContext};
import org.apache.spark.sql.types.{StructType, StructField};
import org.apache.spark.sql.types.IntegerType;
import org.scalatest.{BeforeAndAfter, FunSuite};

class FilterSuite extends FunSuite with SharedSparkContext with BeforeAndAfter {
  var dataRDD: RDD[Row] = null;
  var constraints: Array[Integer] = null;

  before {
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val schema = StructType(Array(
      StructField("value", IntegerType, false)
    ));
    dataRDD = sqlContext.createDataFrame(
      sc.parallelize((1 to 20).toArray.map(Row(_))), schema).rdd;
    constraints = Array(2, 3, 5, 7, 11, 13, 17, 19);
  }

  test("static inRDD") {
    val expected = constraints;
    val reduced = Filter.inRDD(dataRDD, "value", sc.parallelize(constraints))
      .map(_.getAs[Integer]("value"))
      .collect;
    assert(expected.sameElements(reduced.sorted));
  }

  test("static inArray") {
    val expected = constraints;
    val reduced = Filter.inArray(dataRDD, "value", constraints)
      .map(_.getAs[Integer]("value"))
      .collect;
    assert(expected.sameElements(reduced.sorted));
  }

  test("implicit type casting(RDD -> Filter)") {
    def hasImplicit(rdd: RDD[Row])
    (implicit conversion: RDD[Row] => Filter = null): Boolean = {
      return conversion != null;
    }

    assert(hasImplicit(dataRDD));
  }

  test("instantiated inRDD") {
    val expected = constraints;
    val reduced = dataRDD.inRDD("value", sc.parallelize(constraints))
      .map(_.getAs[Integer]("value"))
      .collect;
    assert(expected.sameElements(reduced.sorted));
  }

  test("instantiated inArray") {
    val expected = constraints;
    val reduced = dataRDD.inArray("value", constraints)
      .map(_.getAs[Integer]("value"))
      .collect;
    assert(expected.sameElements(reduced.sorted));
  }
}