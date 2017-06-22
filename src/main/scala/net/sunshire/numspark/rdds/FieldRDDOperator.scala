package net.sunshire.numspark.rdds;

import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.{Row, SQLContext};

object FieldRDDOperator {
  def newField(sourceRDD: RDD[Row]) (map: RDD[Row] => RDD[Row]): RDD[Row] = {
    val sc = sourceRDD.sparkContext;
    val sqlContext = SQLContext.getOrCreate(sc)
    val sourceSchema = sourceRDD.first.schema;
    val newFieldRDD = map(sourceRDD);
    val newFieldSchema = newFieldRDD.first.schema;
    val Array(keySchema, fieldSchema) = newFieldSchema.fields;
    val keyName = keySchema.name;
    val sourceDataframe = sqlContext.createDataFrame(sourceRDD, sourceSchema);
    val newFieldDataframe = sqlContext.createDataFrame(newFieldRDD, newFieldSchema);
    return sourceDataframe.join(newFieldDataframe, keyName).rdd;
  }

  implicit def RDD2FieldRDDOperator(rdd: RDD[Row]) = new FieldRDDOperator(rdd);
}

class FieldRDDOperator(sourceRDD: RDD[Row]) {
  private[rdds] def getRDD() = sourceRDD;

  def newField(map: RDD[Row] => RDD[Row]) = FieldRDDOperator.newField(sourceRDD) (map);
}
