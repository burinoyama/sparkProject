package org.vin.commerce.utils

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JdbcUtil {
  private val dataSource: DataSource = init()

  def init() = {
    val prop = new Properties()

    val properties: Properties = PropertiesUtil.load("config.properties")

    prop.setProperty("maxActive", properties.getProperty("jdbc.datasource.size"))
    prop.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    prop.setProperty("username", properties.getProperty("jdbc.user"))
    prop.setProperty("password", properties.getProperty("jdbc.password"))
    prop.setProperty("url", properties.getProperty("jdbc.url"))

    DruidDataSourceFactory.createDataSource(prop)
  }


  def executeUpdate(sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    val connection: Connection = dataSource.getConnection()

    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          pstmt.setObject(1 + i, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  def executeBatchUpdate(sql: String, params: Iterable[Array[Any]]) {
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null

    val connection = dataSource.getConnection()
    if (params != null && params.size > 0) {
      for (index <- 0 to params.size) {
        rtn(index) = executeUpdate(sql, params.toList(index))
      }

    }
    rtn
  }

  def main(args: Array[String]): Unit = {
    JdbcUtil.executeUpdate("insert into category_top10 values(?,?,?,?,?)", Array("aaa", 1, 1, 1, 1))
  }

}
