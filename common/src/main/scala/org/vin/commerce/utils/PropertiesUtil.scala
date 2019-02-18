package org.vin.commerce.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {

  def main(args: Array[String]): Unit = {

    val property: Properties = PropertiesUtil.load("config.properties")

    println(property.getProperty("kafka.broker.list"))
  }


  def load(propertyName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertyName), "utf-8"))
    prop
  }

}
