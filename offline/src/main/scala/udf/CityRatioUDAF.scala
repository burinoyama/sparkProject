package udf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

class CityRatioUDAF extends UserDefinedAggregateFunction{

  // 定义输入
  override def inputSchema: StructType = ???

  // 定义存储类型
  override def bufferSchema: StructType = ???

  //输出类型
  override def dataType: DataType = ???

  //验证，是否相同的输入有相同的输出， 如果是就返回true
  override def deterministic: Boolean = ???

  // 存储的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = ???

  // 更新，对每一条数据做一次更新，输入加入存储
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???

  //合并 每个分区处理完成，会在到driver式进行
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???
}
