package net.sunshire.numspark.dataframes.linalg;

import org.apache.spark.rdd.RDD;
import org.apache.spark.SharedSparkContext;
import org.apache.spark.sql.SQLContext;
import org.scalatest.FunSuite;
import scala.math;

class MatrixSuite extends FunSuite with SharedSparkContext {

  test("dot - self dot") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0),
        ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v");
    val dots = matrix.dot(
      sqlContext, df, df, ("i", "j", "v"), ("j", "i", "v")
    ).collect;
    val dot = dots(0);
    assert(dot.getAs[String]("row") == "0");
    assert(dot.getAs[String]("col") == "0");
    assert(dot.getAs[Double]("dot") == 5.0);
  }

  test("dot - empty dot") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0),
        ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v").where($"i" === "1");

    val dotsCount = matrix.dot(
      sqlContext, df, df, ("i", "j", "v"), ("j", "i", "v")
    ).count;
    assert(dotsCount == 0);
  }

  test("dot - normal dot") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd1 = sc.parallelize(
      Array(
        ("0", "0", 1.0),
        ("0", "1", 2.0)
      )
    );
    val rdd2 = sc.parallelize(
      Array(
        ("0", "0", 3.0),
        ("1", "0", 4.0)
      )
    );
    val (df1, df2) = (rdd1.toDF("i", "j", "v"), rdd2.toDF("i", "j", "v"));
    val dots = matrix.dot(
      sqlContext, df1, df2, ("i", "j", "v"), ("i", "j", "v")
    ).collect;
    assert(dots.size == 1);
    val dot = dots(0);
    assert(dot.getAs[String]("row") == "0");
    assert(dot.getAs[String]("col") == "0");
    assert(dot.getAs[Double]("dot") == 11.0);
  }

  test("norm - invalid norm") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0),
        ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v");
    assertThrows[Exception] {
      matrix.norm(-1, sqlContext, df, "i", "v")
    }
    assertThrows[Exception] {
      matrix.norm(0, sqlContext, df, "i", "v")
    }
  }

  test("norm - 1 norm") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0),
        ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v");

    val norms = matrix.norm(1, sqlContext, df, "i", "v").collect;
    assert(norms.size == 1);
    val norm = norms(0);
    assert(norm.getAs[Double]("norm") == 3.0);
  }

  test("norm - 2 norm") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0),
        ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v");

    val norms = matrix.norm(2, sqlContext, df, "i", "v").collect;
    assert(norms.size == 1);
    val norm = norms(0);
    assert(norm.getAs[Double]("norm") == math.sqrt(5.0));
  }
}