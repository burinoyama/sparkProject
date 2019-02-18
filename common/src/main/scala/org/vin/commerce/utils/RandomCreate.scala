package org.vin.commerce.utils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomCreate {

  def apply[T](param: RanOpt[T]*): Unit = {

  }

  def main(args: Array[String]): Unit = {
    println(generate2(1, 10, 9, ",", false))

  }

  def create(from:Int, to:Int):Int={
    from + new Random().nextInt(to - from)
  }

  def generate(from: Int, to: Int, amount: Int, delimiter: String, allRepeat: Boolean): String = {
    if ((to - from < amount) && !allRepeat)
      "it must be repeat because the amount you give"
    else {
      val list = new ListBuffer[Int]()
      for (i <- 1 until amount) {
        var ele = create(from, to)
        if (!allRepeat) {
          while (list.contains(ele)) {
            ele = create(from, to)
          }
        }
        list.append(ele)
      }
      var result_str = ""
      list.mkString(delimiter)
    }
  }


  def generate2(from: Int, to: Int, amount: Int, delimiter: String, allowRepeat: Boolean): String = {
    if (allowRepeat) {
      val ints = mutable.Queue[Int]()
      val list = new ListBuffer[Int]()
      for (i <- 1 to amount) {
        list.append(create(from, to))
      }
      list.mkString(delimiter)
    } else {
      val set = new mutable.HashSet[Int]()
      while (set.size < amount) {
        set += create(from, to)
      }
      set.mkString(delimiter)
    }
  }

}


case class RanOpt[T](value: T, num: Int) {}
