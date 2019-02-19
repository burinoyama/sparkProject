package org.vin.offline.handler

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.vin.commerce.bean.UserVisitAction
import org.vin.commerce.utils.JdbcUtil
import org.vin.offline.accumulator.CategoryAccumulator
import org.vin.offline.bean.CategoryCount

import scala.collection.mutable

object CategoryTopSessionHandler {

  def handler(userActionRDD: RDD[UserVisitAction], sparkSession: SparkSession, taskId: String): List[CategoryCount] = {

    val accumulator: CategoryAccumulator = new CategoryAccumulator()

//    userActionRDD.foreach(ele => println((ele.order_product_ids + "|" +ele.click_category_id + "|" + ele.order_category_ids + "|" + ele.pay_category_ids)))

    sparkSession.sparkContext.register(accumulator)

    userActionRDD.foreach { ua =>
      if (ua.click_category_id != -1L) {
        accumulator.add(ua.click_category_id + "_click")
      } else if (ua.order_category_ids != null && ua.order_category_ids.size > 0) {
        ua.order_category_ids.split(",").foreach(cid =>
          accumulator.add(cid + "_order")
        )
      } else if (ua.pay_category_ids != null && ua.pay_category_ids.size > 0) {
        ua.pay_category_ids.split(",").foreach(pid =>
          accumulator.add(pid + "_pay")
        )
      }
    }


    val map: mutable.HashMap[String, Long] = accumulator.value

    println(map.mkString("\n"))

    val actionCountByCidMap: Map[String, mutable.HashMap[String, Long]] = map.groupBy({ case (key, count) => key.split("_")(0) })

    val list: List[CategoryCount] = actionCountByCidMap.map { case (cid, actionmap) =>
      CategoryCount(taskId, cid, actionmap.getOrElse(cid + "_click", 0L), actionmap.getOrElse(cid + "_order", 0L), actionmap.getOrElse(cid + "_pay", 0L))
    }.toList

    // 5 对结果进行排序，截取
    val sortedList = list.sortWith { (c1, c2) =>
      if (c1.clickCount > c2.clickCount) {
        true
      } else if (c1.clickCount == c2.clickCount) {
        if (c1.orderCount > c2.orderCount) {
          true
        } else if (c1.orderCount == c2.orderCount) {
          if (c1.payCount > c1.payCount) {
            true
          } else false
        } else false
      } else false
    }.take(10)



//    println(s"sortedCategoryCounterList=${list.mkString("\n")}")

    val result: List[Array[Any]] = sortedList.map(c => Array(c.taskId, c.categoryId, c.clickCount, c.orderCount, c.payCount))

    JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)", result)

    list
  }

}
