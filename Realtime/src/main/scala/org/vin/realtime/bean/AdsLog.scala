package org.vin.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class AdsLog(time:Long, area:String, city:String, userId:Long, adId:Long) {

  def getAdDate() = {
    new SimpleDateFormat("yyyy-MM-dd").format(new Date(time))
  }

}
