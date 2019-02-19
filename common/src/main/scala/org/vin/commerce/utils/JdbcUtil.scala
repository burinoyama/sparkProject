package org.vin.commerce.utils

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

import scala.collection.mutable.ListBuffer

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
    } finally {
      connection.close()
      pstmt.close()
    }
    rtn
  }

//  def executeBatchUpdate3(sql:String, arguments:Iterable[Array[Any]]): Unit = {
//    var ppstmt: PreparedStatement = null
//    val conn: Connection = dataSource.getConnection()
//    var returnValue: ListBuffer[Int] = null
//    if (arguments != null && arguments.size > 0) {
//      conn.setAutoCommit(false)
//      val statement: Statement = conn.createStatement()
//      ppstmt = conn.createStatement()
//      ppstmt.addBatch(sql)
//      ppstmt.1
//    }
//  }

  def executeBatchUpdate(sql: String, arguments: Iterable[Array[Any]]): Unit = {
    var ppstmt: PreparedStatement = null
    val conn: Connection = dataSource.getConnection()
    var returnValue: ListBuffer[Int] = null
    if (arguments != null && arguments.size > 0) {
      returnValue = new ListBuffer[Int]
      conn.setAutoCommit(false)
      for (args <- arguments) {
        try {
          ppstmt = conn.prepareStatement(sql)
          for (i <- 0 until args.length) {
            ppstmt.setObject(i + 1, args(i))
          }
          val exeValue: Int = ppstmt.executeUpdate()
          returnValue.append(exeValue)
        } catch {
          case ex: Exception => ex.printStackTrace()
        } finally {
          ppstmt.close()
        }
      }
      conn.commit()
    }
    returnValue
  }

  def executeBatchUpdat2(sql: String, params: Iterable[Array[Any]]) {
    val rtn: ListBuffer[Int] = new ListBuffer[Int]
    var pstmt: PreparedStatement = null

    //    val connection = dataSource.getConnection()
    if (params != null && params.size > 0) {
      //      for (index <- 0 to params.size) {
      //        println(params.toString())
      //
      //        rtn(index) = executeUpdate(sql, params.toList(index))
      //      }

      for (elem <- params) {
        println(elem.toString)
        val exeRet: Int = executeUpdate(sql = sql, elem)
        rtn.append(exeRet)
      }
    }
    rtn
  }

  def main(args: Array[String]): Unit = {
    JdbcUtil.executeUpdate("insert into category_top10 values(?,?,?,?,?)", Array("streneous", 1, 1, 1, 1))
  }

}
