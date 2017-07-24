package com.yuhe.mgame.log

import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONObject
import collection.mutable.{ Map => MutableMap }
import org.apache.commons.lang.StringUtils
import com.yuhe.mgame.utils.LogUtils
import com.yuhe.mgame.db.{ DBManager, UserInfoDB }

object AddPlayerLog extends LogTrait {
  val LOG_COLS = Array("Uid", "Urs", "PhoneInfo", "SDKInfo", "Name")
  val DB_COLS = Array("HostID", "Uid", "Urs", "PhoneInfo", "Name", "Time")
  val TBL_NAME = "tblAddPlayerLog"

  def parseLog(logList: ArrayBuffer[JSONObject], serverMap: collection.mutable.Map[String, ArrayBuffer[String]], sdkMap: collection.mutable.Map[String, String]) {
    val platformResults = MutableMap[String, ArrayBuffer[MutableMap[String, String]]]()
    for (log <- logList) {
      val hostID = log.getString("hostid")
      val message = log.getString("message")

      if (StringUtils.isNotBlank(message)) {
        val map = MutableMap[String, String]("HostID" -> hostID, "SrcHostID" -> hostID)
        val time = LogUtils.getLogTime(message)
        map("Time") = time
        for (col <- LOG_COLS) {
          val value = LogUtils.getLogValue(message, col, "")
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
      UserInfoDB.insertAddPlayerUser(platformID, platformResult)
    }
  }
}