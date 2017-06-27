package net.sunshire.numspark.rdds;

import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import scala.reflect.ClassTag;

object Filter {
  /**
    * Filter an RDD by another RDD and the given key.
    *
    * @param sourceRDD: RDD[Row]; The RDD which is going to be filtered.
    * @param key: String; The filtering key name.
    * @param rdd: RDD[Row]; The RDD which contains only the filtering constraints.
    * @return RDD[Row]; The RDD after filtered.
    */
  def inRDD[K](sourceRDD: RDD[Row], key: String, rdd: RDD[K])
  (implicit kt: ClassTag[K], ord: Ordering[K] = null): RDD[Row] = {
    val keyedRDD = rdd.distinct.map(value => (value, null));
    val filteredRDD = sourceRDD.map( row =>
      (row.getAs[K](key), row)
    )
    .join(keyedRDD)
    .map{ case (key, (row, nothing)) => row };

    return filteredRDD;
  }

  /**
    * Filter an RDD by an array and the given key.
    *
    * @param sourceRDD: RDD[Row]; The RDD which is going to be filtered.
    * @param key: String; The filtering key name.
    * @param array: Array; The array which contains only the filtering constraints.
    * @return RDD[Row]; The RDD after filtered.
    */
  def inArray[K](sourceRDD: RDD[Row], key: String, array: Array[K])
  (implicit kt: ClassTag[K]): RDD[Row] = {
    val sc = sourceRDD.context;
    val br = sc.broadcast(array.distinct);
    val filteredRDD = sourceRDD.filter(row => br.value.contains(row.getAs[K](key)));

    return filteredRDD;
  }

  /**
    * Implicitly converts RDD to Filter class
    *
    * @param rdd: RDD[Row]; the source RDD.
    * @return Filter
    */
  implicit def RDD2Filter(rdd: RDD[Row]) = new Filter(rdd);
}

class Filter(sourceRDD: RDD[Row]) {
  private[rdds] def getRDD = sourceRDD;

  /**
    * Filter this RDD by another RDD and the given key.
    *
    * @param key: String; The filtering key name.
    * @param rdd: RDD[Row]; The RDD which contains only the filtering constraints.
    * @return RDD[Row]; The RDD after filtered.
    */
  def inRDD[K](key: String, rdd: RDD[K])
  (implicit kt: ClassTag[K], ord: Ordering[K] = null): RDD[Row] =
    Filter.inRDD(sourceRDD, key, rdd);

  /**
    * Filter this RDD by an array and the given key.
    *
    * @param key: String; The filtering key name.
    * @param array: Array; The array which contains only the filtering constraints.
    * @return RDD[Row]; The RDD after filtered.
    */
  def inArray[K](key: String, rdd: Array[K])
  (implicit kt: ClassTag[K]): RDD[Row] = Filter.inArray(sourceRDD, key, rdd);
}