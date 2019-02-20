package org.vin.offline.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import udf.CityRatioUDAF

object AreaCountApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("area_count").setMaster("local[*]")

    val sparkSession: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    sparkSession.udf.register("city_ratio", new CityRatioUDAF)

    count(sparkSession)
  }

  def count(sparkSession: SparkSession): Unit = {
    sparkSession.sql("use sparkmall")

    val join_city = "" +
      "select uva.click_product_id, ci.area, ci.city_name " +
      "from user_visit_action uva inner join city_info ci " +
      "where uva.city_id = ci.city_id and uva.click_product_id > 0"
    sparkSession.sql(join_city).createOrReplaceTempView("tb_original_view")

    val aggr_count = "select area, click_product_id as pid, count(*) as click_count, city_ratio(city_name) as city_remark" +
      " from tb_original_view " +
      " group by area, click_product_id"

    sparkSession.sql(aggr_count).createOrReplaceTempView("tb_area_id_count")
//    sparkSession.sql(aggr_count).show(100)

    // 开窗函数作为一个值
    val sort_in_group = "select area, pid, click_count, city_remark from " +
      " (select v.*, rank()over(partition by area order by click_count desc)rk " +
      " from tb_area_id_count v)clickrk " +
      " where rk <= 3"

    sparkSession.sql(sort_in_group).createOrReplaceTempView("tb_before_finished")


    val join_product = "select tbf.area, pi.product_name, tbf.click_count, tbf.city_remark " +
      "from tb_before_finished tbf inner join product_info pi " +
      "where tbf.pid = pi.product_id"

    sparkSession.sql(join_product).show(100)



  }

}
