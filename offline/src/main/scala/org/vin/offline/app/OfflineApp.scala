package org.vin.offline.app

import java.util.UUID

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.vin.commerce.bean.UserVisitAction
import org.vin.commerce.utils.PropertiesUtil
import org.vin.offline.bean.CategoryCount
import org.vin.offline.handler.{CategoryCountTopSessionHandler, CategoryTopSessionHandler}

object OfflineApp {

  def main(args: Array[String]): Unit = {

    val taskId: String = UUID.randomUUID().toString

    val sparkConf: SparkConf = new SparkConf().setAppName("spark_mall_offline").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val properties = PropertiesUtil.load("conditions.properties")

    val conditionJson = properties.getProperty("condition.params.json")

    val jsonObject = JSON.parseObject(conditionJson)

    val unit: RDD[UserVisitAction] = readUserVisitActionToRDD(sparkSession, jsonObject, "sparkmall")

    val counts: List[CategoryCount] = CategoryTopSessionHandler.handler(unit, sparkSession, taskId)

    println("finish the project first")

    val secondResult: Unit = CategoryCountTopSessionHandler.handler(sparkSession, unit, taskId, counts)


    println("finish the project second")

    sparkSession.close()

  }


  def readUserVisitActionToRDD(sparkSession: SparkSession, jObject: JSONObject, database: String) = {
    val startDate = jObject.get("startDate")
    val endDate = jObject.get("endDate")
    val sql = new StringBuilder("select uva.*")
      .append(" from user_visit_action uva left join user_info uf on uva.user_id = uf.user_id")
      .append(" where 1 = 1")
      .append(" and uva.date > '" + startDate + "'")
      .append(" and uva.date < '" + endDate + "'")
    if (!jObject.getString("startAge").isEmpty) {
      sql.append(" and uf.age > '" + jObject.getString("startAge") + "'")
    }
    if (!jObject.getString("endAge").isEmpty) {
      sql.append(" and uf.age < '" + jObject.getString("endAge") + "'")
    }
    if (!jObject.getString("professionals").isEmpty) {
      sql.append(" and uf.professional = '" + jObject.getString("professionals") + "'")
    }
    if (!jObject.getString("city").isEmpty) {
      sql.append(" and uf.city = '" + jObject.getString("city") + "'")
    }

    //TODO

    println(sql.toString())
    if (!database.isEmpty) {
      sparkSession.sql("use " + database)
    }
    import sparkSession.implicits._
    val rdd = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd

//    rdd.foreach(ele => println((ele.click_category_id + "|" + ele.order_category_ids + "|" + ele.pay_category_ids)))

    rdd

  }
}
