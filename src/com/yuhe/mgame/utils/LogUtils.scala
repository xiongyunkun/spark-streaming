package com.yuhe.mgame.utils

import scala.util.matching.Regex

object LogUtils {
  /**
   * 获得日志字段值
   */
  def getLogValue(logLine:String, key:String, defaultValue:String) = {
    val pattern = ("[),]"+ key + "=(.*?)(,|$)").r
    var value = defaultValue
    val findIn = pattern.findAllIn(logLine)
    if(findIn.hasNext){
      val pattern(tValue, _) = findIn.next
      value = tValue
    }
    value
  }
  /**
   * 获得日志时间
   */
  def getLogTime(logLine:String) = {
    val pattern = "^\\[(.*?)\\]".r
    var time = ""
    val findIn = pattern.findAllIn(logLine)
    if(findIn.hasNext){
      val pattern(tValue) = findIn.next
      time = tValue
    }
    time
  }
  /**
   * 根据SDKID获得对应的渠道
   */
  def getPlatformIDBySDKID(sdkInfo:String, sdkMap:collection.mutable.Map[String, String]) = {
    //默认渠道，根据实际情况修改
    var platformID = "test"
    val sdkArray = sdkInfo.split("_")
    if(sdkArray.length >= 1){
      val sdkID = sdkArray(0)
      if(sdkMap.contains(sdkID)){
        platformID = sdkMap(sdkID)
      }
    }
    platformID
  }
  
  def main(args:Array[String]){
    val logLine = "[2017-07-08 15:58:35] (2)Urs=hgh001,Uid=6780800,Level=1,Name=hgh"
    val value = getLogValue(logLine, "Urs", "unkown")
    val time = getLogTime(logLine)
    println(value)
    println(time)
  }
}