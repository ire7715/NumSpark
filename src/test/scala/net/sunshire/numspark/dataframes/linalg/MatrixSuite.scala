package net.sunshire.numspark.dataframes.linalg;

import org.apache.spark.rdd.RDD;
import org.apache.spark.SharedSparkContext;
import org.apache.spark.sql.SQLContext;
import org.scalatest.FunSuite;
import scala.math;

class MatrixSuite extends FunSuite with SharedSparkContext {

  test("dot - self vector dot") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0),
        ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v");
    val dots = matrix.dot((df, ("i", "j", "v")), (df, ("j", "i", "v"))).collect;
    val dot = dots(0);
    assert(dot.getAs[String]("row") == "0");
    assert(dot.getAs[String]("col") == "0");
    assert(dot.getAs[Double]("dot") == 5.0);
  }

  test("dot - empty vector dot") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0),
        ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v").where($"i" === "1");

    val dotsCount = matrix.dot((df, ("i", "j", "v")), (df, ("j", "i", "v"))).count;
    assert(dotsCount == 0);
  }

  test("dot - vector dot") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd1 = sc.parallelize(
      Array(
        ("0", "0", 1.0), ("0", "1", 2.0)
      )
    );
    val rdd2 = sc.parallelize(
      Array(
        ("0", "0", 3.0),
        ("1", "0", 4.0)
      )
    );
    val (df1, df2) = (rdd1.toDF("i", "j", "v"), rdd2.toDF("i", "j", "v"));
    val dots = matrix.dot((df1, ("i", "j", "v")), (df2, ("i", "j", "v"))).collect;
    assert(dots.size == 1);
    val dot = dots(0);
    assert(dot.getAs[String]("row") == "0");
    assert(dot.getAs[String]("col") == "0");
    assert(dot.getAs[Double]("dot") == 11.0);
  }

  test("dot - matrix dot") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd1 = sc.parallelize(
      Array(
        ("0", "0", 1.0), ("0", "1", 2.0),
        ("1", "0", 3.0), ("1", "1", 4.0)
      )
    );
    val rdd2 = sc.parallelize(
      Array(
        ("0", "0", 5.0), ("0", "1", 6.0),
        ("1", "0", 7.0), ("1", "1", 8.0)
      )
    );
    val (df1, df2) = (rdd1.toDF("i", "j", "v"), rdd2.toDF("i", "j", "v"));
    val dots = matrix.dot((df1, ("i", "j", "v")), (df2, ("i", "j", "v")))
      .orderBy($"row", $"col").collect;
    assert(dots.size == 4);
    assert(dots(0).getAs[String]("row") == "0");
    assert(dots(0).getAs[String]("col") == "0");
    assert(dots(0).getAs[Double]("dot") == 19.0);
    assert(dots(1).getAs[String]("row") == "0");
    assert(dots(1).getAs[String]("col") == "1");
    assert(dots(1).getAs[Double]("dot") == 22.0);
    assert(dots(2).getAs[String]("row") == "1");
    assert(dots(2).getAs[String]("col") == "0");
    assert(dots(2).getAs[Double]("dot") == 43.0);
    assert(dots(3).getAs[String]("row") == "1");
    assert(dots(3).getAs[String]("col") == "1");
    assert(dots(3).getAs[Double]("dot") == 50.0);
  }

  test("norm - invalid norm") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0), ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v");
    assertThrows[Exception] {
      matrix.norm(-1, df, "i", "v")
    }
    assertThrows[Exception] {
      matrix.norm(0, df, "i", "v")
    }
  }

  test("norm - 1 norm") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0), ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v");

    val norms = matrix.norm(1, df, "i", "v").collect;
    assert(norms.size == 1);
    val norm = norms(0);
    assert(norm.getAs[Double]("norm") == 3.0);
  }

  test("norm - 2 norm") {
    val sqlContext: SQLContext = new SQLContext(sc);
    import sqlContext.implicits._;

    val rdd = sc.parallelize(
      Array(
        ("0", "0", 1.0), ("0", "1", 2.0)
      )
    );
    val df = rdd.toDF("i", "j", "v");

    val norms = matrix.norm(2, df, "i", "v").collect;
    assert(norms.size == 1);
    val norm = norms(0);
    assert(norm.getAs[Double]("norm") == math.sqrt(5.0));
  }
}