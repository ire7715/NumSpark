package net.sunshire.numspark.rdds;

import net.sunshire.numspark.rdds.Conversions._;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.{Row, SQLContext};

object FieldRDDOperator {
  /**
    * Append a new field to the sourceRDD.
    *
    * @param sourceRDD: RDD[Row]
    * @param map: RDD[Row] => RDD[Row]; The input schema is same as sourceRDD, the output schema is (key, newField). The key shall match the key column in sourceRDD
    * @return RDD[Row]; an RDD that append the mapped column to sourceRDD.
    */
  def newField(sourceRDD: RDD[Row]) (map: RDD[Row] => RDD[Row]): RDD[Row] = {
    val newFieldRDD = map(sourceRDD);
    val Array(keySchema, _) = newFieldRDD.first.schema.fields;
    return sourceRDD.join(newFieldRDD, keySchema.name).rdd;
  }

  /**
    * Perform the given reduction algorithm with given the input columns. It appends preserved columns after the reduction is done.
    * If you dismiss the key and inputParams, this function passes the whole sourceRDD into *reduce* function.
    * By dismissing the preservedParams, this funciton append no columns after the reducion is done.
    *
    * @param sourceRDD: RDD[Row]
    * @param key: String; The primary key to join reduced data and preserved data.
    * @param inputParams: Array[String]; Columns needed for reduction, key excluded.
    * @param preservedParams: Array[String]; Columns appended after the reduction is done.
    * @param reduce: RDD[Row] => RDD[Row]
    * @return RDD[Row]; an RDD that reduce sourceRDD via the given function. Append preserved columns if assigned.
    */
  def reduceFields(
      sourceRDD: RDD[Row],
      key: String = "",
      inputParams: Array[String] = Array[String](),
      preservedParams: Array[String] = Array[String]())
      (reduce: RDD[Row] => RDD[Row]): RDD[Row] = {
    val inputRDD =
      if (inputParams.length == 0) sourceRDD
      else sourceRDD.select(key, inputParams: _*).rdd;
    val reducedRDD = reduce(inputRDD);

    return if (preservedParams.length == 0) reducedRDD;
      else return reducedRDD.join(sourceRDD.select(key, preservedParams: _*), key).rdd;
  }

  /**
    * An implicit function to convert RDD[Row] to FieldRDDOperator.
    *
    * @param rdd: RDD[Row]
    * @return FieldRDDOperator
    */
  implicit def RDD2FieldRDDOperator(rdd: RDD[Row]) = new FieldRDDOperator(rdd);
}

class FieldRDDOperator(sourceRDD: RDD[Row]) {
  private[rdds] def getRDD() = sourceRDD;

  /**
    * Append a new field to the sourceRDD. (same as FieldRDDOperator.newField, just putting this RDD as the sourceRDD)
    *
    * @param map: RDD[Row] => RDD[Row]; The input schema is same as sourceRDD, the output schema is (key, newField). The key shall match the key column in sourceRDD
    * @return RDD[Row]; an RDD that append the mapped column to sourceRDD.
    */
  def newField(map: RDD[Row] => RDD[Row]) = FieldRDDOperator.newField(sourceRDD) (map);

  /**
    * Perform the given reduction algorithm with given the input columns. It appends preserved columns after the reduction is done.
    * If you dismiss the key and inputParams, this function passes the whole sourceRDD into *reduce* function.
    * By dismissing the preservedParams, this funciton append no columns after the reducion is done.
    * (same as FieldRDDOperator.reduceFields, just putting this RDD as the sourceRDD)
    *
    * @param key: String; The primary key to join reduced data and preserved data.
    * @param inputParams: Array[String]; Columns needed for reduction, key excluded.
    * @param preservedParams: Array[String]; Columns appended after the reduction is done.
    * @param reduce: RDD[Row] => RDD[Row]
    * @return RDD[Row]; an RDD that reduce sourceRDD via the given function. Append preserved columns if assigned.
    */
  def reduceFields(
      key: String = "",
      inputParams: Array[String] = Array[String](),
      preservedParams: Array[String] = Array[String]())
      (reduce: RDD[Row] => RDD[Row]): RDD[Row] =
    FieldRDDOperator.reduceFields(sourceRDD, key, inputParams, preservedParams) (reduce);
}
