package net.sunshire.numspark.rdds;

import com.holdenkarau.spark.testing.SharedSparkContext;
import net.sunshire.numspark.rdds.FieldRDDOperator._;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SQLContext};
import org.apache.spark.sql.types.{StructType, StructField};
import org.apache.spark.sql.types.IntegerType;
import org.scalatest.{BeforeAndAfter, FunSuite};

class FieldRDDOperatorSuite
    extends FunSuite with SharedSparkContext with BeforeAndAfter {
  val data = Array(1, 2, 3);
  val dataSchema = StructType(Array(
    StructField("valueA", IntegerType, false),
    StructField("valueB", IntegerType, false),
    StructField("valueC", IntegerType, false),
    StructField("valueD", IntegerType, false)
  ));
  var dataRDD: RDD[Row] = null;

  before {
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._;
    dataRDD = sqlContext.createDataFrame(
      sc.parallelize(data.map(v =>
        Row(v, v * 5, v * 7, v * 11))), dataSchema).rdd;
  }

  test("implicit type casting(RDD -> FieldRDDOperator)") {
    def hasImplicit(rdd: RDD[Row])
        (implicit conversion: RDD[Row] => Filter = null): Boolean = {
      return conversion != null;
    }

    assert(hasImplicit(dataRDD));
  }

  test("static new field") {
    val expectedSchema = StructType(Array(
      StructField("valueA", IntegerType, false),
      StructField("valueB", IntegerType, false),
      StructField("valueC", IntegerType, false),
      StructField("valueD", IntegerType, false),
      StructField("valueZ", IntegerType, false)
    ));
    val expectedArray = data.map(v => Row(v, v * 5, v * 101));

    val newFieldArray = FieldRDDOperator.newField(dataRDD) { sourceRDD =>
      val sc = sourceRDD.sparkContext;
      val sqlContext = new SQLContext(sc);
      import sqlContext.implicits._;
      val newFieldSchema = StructType(Array(
        StructField("valueA", IntegerType, false),
        StructField("valueZ", IntegerType, false)
      ));
      val newFieldRDD = sourceRDD.map { row =>
        val valueA = row.getAs[Integer]("valueA");
        Row(valueA, valueA * 101)
      };
      sqlContext.createDataFrame(newFieldRDD, newFieldSchema).rdd;
    }.collect.sortBy(_.getAs[Integer]("valueA"));

    for(pair <- expectedSchema.fields.zip(newFieldArray(0).schema.fields))
      assert(pair._1 == pair._2);
    for(pair <- expectedArray.zip(newFieldArray)) {
      assert(pair._1.getAs[Integer](0) == pair._2.getAs[Integer]("valueA"));
      assert(pair._1.getAs[Integer](1) == pair._2.getAs[Integer]("valueB"));
      assert(pair._1.getAs[Integer](2) == pair._2.getAs[Integer]("valueZ"));
    }
  }

  test("instantiated new field") {
    val expectedSchema = StructType(Array(
      StructField("valueA", IntegerType, false),
      StructField("valueB", IntegerType, false),
      StructField("valueC", IntegerType, false),
      StructField("valueD", IntegerType, false),
      StructField("valueZ", IntegerType, false)
    ));
    val expectedArray = data.map(v => Row(v, v * 5, v * 101));

    val newFieldArray = dataRDD.newField { sourceRDD =>
      val sc = sourceRDD.sparkContext;
      val sqlContext = new SQLContext(sc);
      import sqlContext.implicits._;
      val newFieldSchema = StructType(Array(
        StructField("valueA", IntegerType, false),
        StructField("valueZ", IntegerType, false)
      ));
      val newFieldRDD = sourceRDD.map { row =>
        val valueA = row.getAs[Integer]("valueA");
        Row(valueA, valueA * 101)
      };
      sqlContext.createDataFrame(newFieldRDD, newFieldSchema).rdd;
    }.collect.sortBy(_.getAs[Integer]("valueA"));

    for(pair <- expectedSchema.fields.zip(newFieldArray(0).schema.fields))
      assert(pair._1 == pair._2);
    for(pair <- expectedArray.zip(newFieldArray)) {
      assert(pair._1.getAs[Integer](0) == pair._2.getAs[Integer]("valueA"));
      assert(pair._1.getAs[Integer](1) == pair._2.getAs[Integer]("valueB"));
      assert(pair._1.getAs[Integer](2) == pair._2.getAs[Integer]("valueZ"));
    }
  }

  test("static reduce fields w/ input and preservation") {
    val expectedSchema = StructType(Array(
      StructField("valueA", IntegerType, false),
      StructField("valueBC", IntegerType, false),
      StructField("valueD", IntegerType, false)
    ));
    val expectedArray = data.map(v => Row(v, v * 5 + v * 7, v * 11));

    val reducedFieldsArray = FieldRDDOperator.reduceFields(
        dataRDD, "valueA", Array("valueB", "valueC"), Array("valueD")) { inputRDD =>
      val sc = inputRDD.sparkContext;
      val sqlContext = new SQLContext(sc);
      import sqlContext.implicits._;
      val inputDataframe = sqlContext.createDataFrame(inputRDD, inputRDD.first.schema);
      inputDataframe.select(
        $"valueA", expr("valueB + valueC").alias("valueBC")).rdd;
    }.collect.sortBy(_.getAs[Integer]("valueA"));

    for (pair <- expectedSchema.fields.zip(reducedFieldsArray(0).schema.fields))
      assert(pair._1 == pair._2)
    for (pair <- expectedArray.zip(reducedFieldsArray)) {
      assert(pair._1.getAs[Integer](0) == pair._2.getAs[Integer]("valueA"));
      assert(pair._1.getAs[Integer](1) == pair._2.getAs[Integer]("valueBC"));
      assert(pair._1.getAs[Integer](2) == pair._2.getAs[Integer]("valueD"));
    }
  }

  test("static reduce fields w/o input and preservation") {
    val expectedSchema = StructType(Array(
      StructField("valueA", IntegerType, false),
      StructField("valueBC", IntegerType, false)
    ));
    val expectedArray = data.map(v => Row(v, v * 5 + v * 7));

    val reducedFieldsArray = FieldRDDOperator.reduceFields(
        dataRDD) { inputRDD =>
      val sc = inputRDD.sparkContext;
      val sqlContext = new SQLContext(sc);
      import sqlContext.implicits._;
      val inputDataframe = sqlContext.createDataFrame(inputRDD, inputRDD.first.schema);
      inputDataframe.select(
        $"valueA", expr("valueB + valueC").alias("valueBC")).rdd;
    }.collect.sortBy(_.getAs[Integer]("valueA"));

    for (pair <- expectedSchema.fields.zip(reducedFieldsArray(0).schema.fields))
      assert(pair._1 == pair._2)
    for (pair <- expectedArray.zip(reducedFieldsArray)) {
      assert(pair._1.getAs[Integer](0) == pair._2.getAs[Integer]("valueA"));
      assert(pair._1.getAs[Integer](1) == pair._2.getAs[Integer]("valueBC"));
    }
  }

  test("instantiated reduce fields w/ input and preservation") {
    val expectedSchema = StructType(Array(
      StructField("valueA", IntegerType, false),
      StructField("valueBC", IntegerType, false),
      StructField("valueD", IntegerType, false)
    ));
    val expectedArray = data.map(v => Row(v, v * 5 + v * 7, v * 11));

    val reducedFieldsArray = dataRDD.reduceFields(
        "valueA", Array("valueB", "valueC"), Array("valueD")) { inputRDD =>
      val sc = inputRDD.sparkContext;
      val sqlContext = new SQLContext(sc);
      import sqlContext.implicits._;
      val inputDataframe = sqlContext.createDataFrame(inputRDD, inputRDD.first.schema);
      inputDataframe.select(
        $"valueA", expr("valueB + valueC").alias("valueBC")).rdd;
    }.collect.sortBy(_.getAs[Integer]("valueA"));

    for (pair <- expectedSchema.fields.zip(reducedFieldsArray(0).schema.fields))
      assert(pair._1 == pair._2)
    for (pair <- expectedArray.zip(reducedFieldsArray)) {
      assert(pair._1.getAs[Integer](0) == pair._2.getAs[Integer]("valueA"));
      assert(pair._1.getAs[Integer](1) == pair._2.getAs[Integer]("valueBC"));
      assert(pair._1.getAs[Integer](2) == pair._2.getAs[Integer]("valueD"));
    }
  }

  test("instantiated reduce fields w/o input and preservation") {
    val expectedSchema = StructType(Array(
      StructField("valueA", IntegerType, false),
      StructField("valueBC", IntegerType, false)
    ));
    val expectedArray = data.map(v => Row(v, v * 5 + v * 7));

    val reducedFieldsArray = dataRDD.reduceFields() { inputRDD =>
      val sc = inputRDD.sparkContext;
      val sqlContext = new SQLContext(sc);
      import sqlContext.implicits._;
      val inputDataframe = sqlContext.createDataFrame(inputRDD, inputRDD.first.schema);
      inputDataframe.select(
        $"valueA", expr("valueB + valueC").alias("valueBC")).rdd;
    }.collect.sortBy(_.getAs[Integer]("valueA"));

    for (pair <- expectedSchema.fields.zip(reducedFieldsArray(0).schema.fields))
      assert(pair._1 == pair._2)
    for (pair <- expectedArray.zip(reducedFieldsArray)) {
      assert(pair._1.getAs[Integer](0) == pair._2.getAs[Integer]("valueA"));
      assert(pair._1.getAs[Integer](1) == pair._2.getAs[Integer]("valueBC"));
    }
  }
}
