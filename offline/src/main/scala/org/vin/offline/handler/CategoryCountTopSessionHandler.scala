package org.vin.offline.handler

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.vin.commerce.bean.UserVisitAction
import org.vin.commerce.utils.JdbcUtil
import org.vin.offline.bean.CategoryCount

object CategoryCountTopSessionHandler {


  // 获得top10中每个品类的点击前三的用户的session_id
  def handler(sparkSession: SparkSession, userActionRdd: RDD[UserVisitAction], taskId: String, top10CategoryList: List[CategoryCount]): Unit = {
    //1 获得top10 的品类的product_id
    //2 通过product_id 获得所有的相关的session_id
    //3 以product_id为键， 包含点击次数和session_id的数据为value

    val leftIds: List[String] = top10CategoryList.map(_.categoryId)

    val leftIdsBC: Broadcast[List[String]] = sparkSession.sparkContext.broadcast(leftIds)

    val relatedRdd: RDD[UserVisitAction] = userActionRdd.filter(action => {
      leftIdsBC.value.contains(action.click_category_id.toString) // maybe some field not be negative
    })

    val kv_data: RDD[(String, Long)] = relatedRdd.map(action => {
      (action.click_category_id + "_" + action.session_id, 1L)
    }).reduceByKey(_ + _)

    // 分组，以cid为依据
    val sortedByCidRDD: RDD[(String, Iterable[(String, Long)])] = kv_data.map { case (cidsession, count) =>
      val multi: Array[String] = cidsession.split("_")
      val cid: String = multi(0)
      val session: String = multi(1)
      (cid, (session, count))
    }.groupByKey()

    // 组内排序，小组赛
    val unit: RDD[List[(List[String], String, String, Long)]] = sortedByCidRDD.map { case (cid, sessionCount) =>
      val sortedSessionCountRDD: List[(String, Long)] = sessionCount.toList.sortWith((sc1, sc2) =>
        sc1._2 > sc2._2
      ).take(10)
      val result: List[(List[String], String, String, Long)] = sortedSessionCountRDD.map { case (k, v) =>
        (leftIdsBC.value, cid, k, v)
      }
      result
    }

    val value: RDD[Array[Any]] = sortedByCidRDD.flatMap { case (cid, sessionCount) =>
      val sortedSessionCountRDD: List[(String, Long)] = sessionCount.toList.sortWith { case (s1, s2) =>
        s1._2 > s2._2
      }.take(100)
      val list: List[Array[Any]] = sortedSessionCountRDD.map { case (k, v) =>
        Array(taskId, cid, k, v)
      }
      list
    }
    val array: Array[Array[Any]] = value.collect()

    JdbcUtil.executeBatchUpdate("insert into category_top10_sessiontop10 values(?, ?, ?, ?)", array)

    array
  }

}
