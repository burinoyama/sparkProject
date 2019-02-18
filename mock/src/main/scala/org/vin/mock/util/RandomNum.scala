package org.vin.mock.util

import org.vin.commerce.utils.RandomCreate.create

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object RandomNum {

  def apply(fromNum:Int,toNum:Int): Int =  {
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }

  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={
    if (canRepeat) {
      val ints = mutable.Queue[Int]()
      val list = new ListBuffer[Int]()
      for (i <- 1 to amount) {
        list.append(create(fromNum, toNum))
      }
      list.mkString(delimiter)
    } else {
      val set = new mutable.HashSet[Int]()
      while (set.size < amount) {
        set += create(fromNum, toNum)
      }
      set.mkString(delimiter)
    }
  }

}
