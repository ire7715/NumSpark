package net.sunshire.numspark.dataframes.features

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

class FlattenVectorSuite extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var sqlContext: SQLContext = _
  before {
    sqlContext = new SQLContext(sc)
  }

  test("flatten") {
    val expectedSchema = StructType(Seq(
      StructField("id", StringType),
      StructField("output1_0", DoubleType),
      StructField("output1_1", DoubleType),
      StructField("output1_2", DoubleType),
      StructField("output2_0", DoubleType),
      StructField("output2_1", DoubleType),
      StructField("output2_2", DoubleType)
    ))
    val expected = Seq(
      Seq("A", 0.0, -2.0, 2.3, -2.0, 2.3, 0.0),
      Seq("B", -2.0, 2.3, 0.0, 0.0, -2.0, 2.3)
    )
    val defaultAttr = NumericAttribute.defaultAttr
    val attrs = Array("a", "b", "c").map(defaultAttr.withName)
    val attrGroup1 = new AttributeGroup("features1", attrs.asInstanceOf[Array[Attribute]])
    val attrGroup2 = new AttributeGroup("features2", attrs.asInstanceOf[Array[Attribute]])
    val schema = StructType(Seq(
      StructField("id", StringType),
      attrGroup1.toStructField,
      attrGroup2.toStructField
    ))
    val data = sqlContext.createDataFrame(sc.parallelize(Seq(
      Row("A", Vectors.sparse(3, Seq((1, -2.0), (2, 2.3))), Vectors.dense(-2.0, 2.3, 0.0)),
      Row("B", Vectors.dense(-2.0, 2.3, 0.0), Vectors.sparse(3, Seq((1, -2.0), (2, 2.3))))
    )), schema)
    val resultDF = FlattenVector(data, Map("features1" -> "output1", "features2" -> "output2"))
    val resultSchema = resultDF.schema
    assert(resultSchema.length == expectedSchema.length, "(Schema length doesn't match.)")
    assert(resultDF.count == expected.length, s"(Expected length: ${expected.length}, " +
      s"but got [${resultDF.count}])")
    for ((expectedField, i) <- expectedSchema.zipWithIndex) {
      val resultField = resultSchema(i)
      assert(expectedField.name == resultField.name,
        s"(Expected field[${expectedField.name}], but got [${resultField.name}])")
      assert(expectedField.dataType == resultField.dataType,
        s"(Expected field[${expectedField.name}] is [${expectedField.dataType}], " +
        s"but got [${resultField.dataType}])")
    }
    val result = resultDF.orderBy("id").collect
    val expectedSchemaList = expectedSchema.toList
    for ((expectedRow, i) <- expected.zipWithIndex) {
      val resultRow = result(i)
      for ((expectedValue, j) <- expectedRow.zipWithIndex) {
        val resultValue = resultRow.apply(j)
        val location = (expectedRow(0), expectedRow(1), expectedRow(2), expectedRow(3))
        val fieldName = expectedSchemaList(j).name
        assert(expectedValue == resultValue,
          s"(Expected value of [$location, $fieldName] to be [$expectedValue], " +
          s"but got [$resultValue])")
      }
    }
  }
}
