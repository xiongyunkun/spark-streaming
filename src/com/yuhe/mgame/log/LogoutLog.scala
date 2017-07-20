package com.yuhe.mgame.log

import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONObject
import collection.mutable.{ Map => MutableMap }
import org.apache.commons.lang.StringUtils
import com.yuhe.mgame.utils.LogUtils
import com.yuhe.mgame.db.{ DBManager, UserInfoDB }

object LogoutLog extends LogTrait {

  val LOG_COLS = Array("Uid", "Urs", "OnTime", "Level", "Name", "Gold", "Money", "Ip", "SDKInfo", "PhoneInfo")
  val DB_COLS = Array("HostID", "Uid", "Urs", "OnTime", "Level", "Name", "Gold", "Money", "Ip", "PhoneInfo", "Time")
  val TBL_NAME = "tblLogoutLog"
  val defaultValues = Map("Gold" -> "0", "Money" -> "0")

  def parseLog(logList: ArrayBuffer[JSONObject], serverMap: collection.mutable.Map[String, String], sdkMap: collection.mutable.Map[String, String]) {
    val platformResults = MutableMap[String, ArrayBuffer[MutableMap[String, String]]]()
    for (log <- logList) {
      val hostID = log.getString("hostid")
      val message = log.getString("message")

      if (StringUtils.isNotBlank(message)) {
        val map = MutableMap[String, String]("HostID" -> hostID, "SrcHostID" -> hostID)
        val time = LogUtils.getLogTime(message)
        map("Time") = time
        for (col <- LOG_COLS) {
          var defaultValue = ""
          if (defaultValues.contains(col)) {
            defaultValue = defaultValues(col)
          }
          val value = LogUtils.getLogValue(message, col, defaultValue)
          map(col) = value
        }
        if (serverMap.contains(hostID)) {
          val platformID = LogUtils.getPlatformIDBySDKID(map("SDKInfo"), sdkMap)
          platformResults(platformID) = platformResults.getOrElse(platformID, ArrayBuffer[MutableMap[String, String]]())
          if (StringUtils.isNotBlank(map("Uid"))) {
            platformResults(platformID) += map
          }
        }
      }
    }
    //记录入库
    for ((platformID, platformResult) <- platformResults) {
      DBManager.batchInsertByDate(platformID, platformResult, DB_COLS, TBL_NAME)
      UserInfoDB.insertLogoutUser(platformID, platformResult)
    }
  }
}