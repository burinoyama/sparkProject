package org.vin.realtime.app

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.vin.commerce.utils.MyKafkaUtil
import org.vin.realtime.bean.AdsLog

object RealtimeApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("realtime").setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    val streamingContext = new StreamingContext(sparkContext, Seconds(5))

    sparkContext.setCheckpointDir("./checkPoint")

    val recordDstream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream("ads_log", streamingContext)

    BlackListHandler.handler(recordDstream.map { rdd =>
      val values: Array[String] = rdd.value().split(" ")
      AdsLog(values(0).toLong, values(1), values(2), values(3).toLong, values(4).toLong)
    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }
}
