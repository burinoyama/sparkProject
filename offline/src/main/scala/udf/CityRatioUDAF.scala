package udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap

class CityRatioUDAF extends UserDefinedAggregateFunction {

  // 定义输入
  override def inputSchema: StructType = StructType(Array(StructField("city_name", StringType)))

  //定义存储类型 类型 Map, Long
  override def bufferSchema: StructType = StructType(Array(StructField("city_count", MapType(StringType, LongType)), StructField("total_count", LongType)))

  //输出类型
  override def dataType: DataType = StringType

  //验证，是否相同的输入有相同的输出， 如果是就返回true
  override def deterministic: Boolean = true

  // 存储的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new HashMap[String, Long]
    buffer(1) = 0L
  }

  // 更新，对每一条数据做一次更新，输入加入存储 UDAF 不支持可变的Map
  override def update(buffer: MutableAggregationBuffer, input: Row) = {
    //    val unit: Any = buffer.get(0)
    val cityCountMap: Map[String, Long] = buffer.getAs[HashMap[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)
    val cityName: String = input.getString(0)

    buffer(1) = totalCount + 1L
    val tuple: (String, Long) = (cityName, (cityCountMap.getOrElse(cityName, 0L) + 1L))
    buffer(0) = cityCountMap + tuple

    // TODO

    //  TODO   cityCountMap(cityName) = cityCountMap.getOrElse(cityName, 0L) + 1

  }

  //合并 每个分区处理完成，会在到driver式进行
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    /**
      * TODO 这里 变量的类型不能指定为HashMap,必须是Map否则报错
      * java.lang.ClassCastException: scala.collection.immutable.Map$EmptyMap$ cannot be cast to scala.collection.immutable.HashMap
      */

    val cityCountMap1: Map[String, Long] = buffer1.getAs[HashMap[String, Long]](0)
    val cityCountMap2: Map[String, Long] = buffer2.getAs[HashMap[String, Long]](0)
    val totalCount1: Long = buffer1.getLong(1)
    val totalCount2: Long = buffer2.getLong(1)


    buffer1(0) = cityCountMap1.foldLeft(cityCountMap2) { case (cityCountMap2, (cityName1, count1)) =>
      cityCountMap2 + (cityName1 -> (cityCountMap2.getOrElse(cityName1, 0L) + count1))
    }

    buffer1(1) = totalCount1 + totalCount2
  }

  override def evaluate(buffer: Row): Any = {
    val cityCountMap: Map[String, Long] = buffer.getAs[Map[String, Long]](0)
    val totalCount: Long = buffer.getLong(1)
    //计算百分比
    val cityRatioInfoList: List[CityRatioInfo] = cityCountMap.map { case (cityName, count) =>
      val ratio = math.round(count.toDouble / totalCount * 1000) / 10
      CityRatioInfo(cityName, ratio)
    }.toList

    // 排序 截取前2
    val cityRatioInfoTop2List: List[CityRatioInfo] = cityRatioInfoList.sortWith { (c1, c2) =>
      c1.cityRatio > c2.cityRatio
    }.take(2)
    cityRatioInfoTop2List
    //    val cityRatioInfoTop2List: List[CityRatioInfo] = cityRatioInfoList.sortBy(_.cityRatio)(Ordering.Double.reverse).take(2)
    //
    //    cityRatioInfoTop2List

    if (cityRatioInfoTop2List.size > 2) {
      var otherRatio = 100D
      cityRatioInfoTop2List.foreach { cityRatioInfoTop2 =>
        otherRatio -= cityRatioInfoTop2.cityRatio
      }
      otherRatio = math.round(otherRatio) / 1D
      cityRatioInfoTop2List :+ CityRatioInfo("other", otherRatio)
    }

    // 拼接字符串
    cityRatioInfoTop2List.mkString(",")

  }


  case class CityRatioInfo(cityName: String, cityRatio: Double) {
    override def toString: String = {
      cityName + ":" + cityRatio + "%"
    }
  }

}
