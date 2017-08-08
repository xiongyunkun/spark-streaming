package com.yuhe.mgame.log

import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONObject
import collection.mutable.{ Map => MutableMap }
import org.apache.commons.lang.StringUtils
import com.yuhe.mgame.utils.LogUtils
import com.yuhe.mgame.db.DBManager

object ClientLoad extends LogTrait {

  val LOG_COLS = Array("Vfd", "Uid", "Urs", "Step", "PhoneInfo", "SDKInfo")
  val DB_COLS = Array("HostID", "Vfd", "Uid", "Urs", "Step", "IMEI", "PhoneInfo", "Time" )
  val TBL_NAME = "tblClientLoadLog"
  
  def parseLog(logList: ArrayBuffer[JSONObject], serverMap: collection.mutable.Map[String, ArrayBuffer[String]], sdkMap: collection.mutable.Map[String, String]) {
    val platformResults = MutableMap[String, ArrayBuffer[MutableMap[String, String]]]()
    for (log <- logList) {
      val hostID = log.getString("hostid")
      val message = log.getString("message")
      if (StringUtils.isNotBlank(message)) {
        val map = MutableMap[String, String]("HostID" -> hostID)
        val time = LogUtils.getLogTime(message)
        map("Time") = time
        for (col <- LOG_COLS) {
          val value = LogUtils.getLogValue(message, col, "")
          map(col) = value
          //还要单独添加SDKInfo
          if(col == "PhoneInfo"){
            val values = value.split(";")
            var imei = "0"
            if(values.length > 7){
              imei = values(7)
            }
            map("IMEI") = imei
          }
        }
        if (serverMap.contains(hostID)) {
          val platformID = LogUtils.getPlatformIDBySDKID(map("SDKInfo"), sdkMap)
          platformResults(platformID) = platformResults.getOrElse(platformID, ArrayBuffer[MutableMap[String, String]]())
          if (StringUtils.isNotBlank(map("Uid"))){
            platformResults(platformID) += map
          }
        }
      }
    }
     //记录入库
    for ((platformID, platformResult) <- platformResults) {
      DBManager.batchInsertByDate(platformID, platformResult, DB_COLS, TBL_NAME)
    }
  }
}