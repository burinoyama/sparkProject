package org.vin.realtime.app

import java.util.Properties

import org.apache.spark.streaming.dstream.DStream
import org.vin.commerce.utils.PropertiesUtil
import org.vin.realtime.bean.AdsLog
import redis.clients.jedis.Jedis

object BlackListHandler {


  def handler(params: DStream[AdsLog]) = {

    val value: DStream[(String, Long)] = params.map { adsLog =>
      (adsLog.getAdDate() + "_" + adsLog.userId + "_" + adsLog.adId, 1L)
    }.reduceByKey(_ + _)

    System.err.println(value)


    value.foreachRDD(rdd => {
      val prop: Properties = PropertiesUtil.load("config.properties")

      rdd.foreachPartition { adsItr =>
        val jedis = new Jedis(prop.getProperty("redis.host"), prop.getProperty("redis.port").toInt)
        adsItr.foreach { case (logkey, count) =>
          val assemble: Array[String] = logkey.split("_")
          val date = assemble(0)
          val userId = assemble(1)
          val adId = assemble(2)
          val key: String = "user_ads_click" + date
          jedis.hincrBy(key, userId + "_" + adId, count)
          val totalCount: String = jedis.hget(key, userId + "_" + adId)
          println(totalCount)

          if (totalCount != null && totalCount.toLong > 100) {
            jedis.sadd("blackList", userId)
          }


        }
      }
    }
    )

  }

}
