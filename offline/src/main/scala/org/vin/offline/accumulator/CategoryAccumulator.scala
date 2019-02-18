package org.vin.offline.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]] {

  private var orignMap = new mutable.HashMap[String, Long]()

  override def isZero: Boolean = orignMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    new CategoryAccumulator()
  }

  override def reset(): Unit = orignMap.clear()

  override def add(v: String): Unit = orignMap(v) = orignMap.getOrElse(v, 0L) + 1L

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    val otherMap = other.value

    orignMap = orignMap.foldLeft(otherMap) { case (otherMap, (key, count)) =>
      otherMap(key) = otherMap.getOrElse(key, 0L) + count //如果key相同进行累加
      otherMap
    }

  }

  override def value: mutable.HashMap[String, Long] = {
    orignMap
  }
}
