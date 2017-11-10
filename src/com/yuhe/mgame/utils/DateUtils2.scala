package com.yuhe.mgame.utils

import java.text.ParseException
import java.util.Calendar
import java.util.Date

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.commons.lang.time.DateUtils

object DateUtils2 {
  
  private val DateFormat = "yyyy-MM-dd"
	private val TimeFormat = "yyyy-MM-dd HH:mm:ss"
	private val SqlDateFormat = "yyyyMMdd"
	
	
  /**
   * 获取数据库表的时间格式
   */
  def getSqlDate(time:String) = {
    val parsePatterns = Array(TimeFormat)
    var dateStr = DateFormatUtils.format(new Date(), SqlDateFormat)
    try {
			val date = DateUtils.parseDate(time, parsePatterns)
			dateStr = DateFormatUtils.format(date, SqlDateFormat)
		} catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
		dateStr
  }
  
  def getFloorTime(timestamp:Long) = {
    // 先获得当天0点的时间戳
    val benCal = Calendar.getInstance()
    benCal.set(Calendar.HOUR_OF_DAY, 0)
		benCal.set(Calendar.SECOND, 0)
		benCal.set(Calendar.MINUTE, 0)
		benCal.set(Calendar.MILLISECOND, 0)
		val benTime = benCal.getTimeInMillis()
		// 当前时间戳
		val cal = Calendar.getInstance()
		var nowTime = cal.getTimeInMillis()
		if(timestamp != -1){
			nowTime = timestamp * 1000
		}
		val diff = nowTime - benTime
		val floor = Math.floorDiv(diff, 300000)
		val floorTime = benTime + floor * 300000
		val timeStr = DateFormatUtils.format(floorTime, TimeFormat)
		timeStr
  }

  def main(args: Array[String]): Unit = {
    val timestamp = 1508148668
    println(getFloorTime(timestamp))
  }
}