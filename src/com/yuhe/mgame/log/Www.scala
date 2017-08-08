package com.yuhe.mgame.log

import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONObject
import collection.mutable.{ Map => MutableMap }
import org.apache.commons.lang.StringUtils
import com.yuhe.mgame.utils.LogUtils
import com.yuhe.mgame.db.DBManager

object Www extends LogTrait {

  val LOG_COLS = Array("IMEI", "Operate", "PhoneInfo")
  val DB_COLS = Array("Step", "IMEI", "PhoneInfo", "Time")
  val TBL_NAME = "tblWwwLog"
  //登陆步骤转换
  val OPERATE_MAP = Map("Open" -> "1", "ShowLogin" -> "2", "Web" -> "3", "ServerList" -> "4")

  def parseLog(logList: ArrayBuffer[JSONObject], serverMap: collection.mutable.Map[String, ArrayBuffer[String]], sdkMap: collection.mutable.Map[String, String]) {
    val platformResults = MutableMap[String, ArrayBuffer[MutableMap[String, String]]]()
    for (log <- logList) {
      val message = log.getString("message")
      if (StringUtils.isNotBlank(message)) {
        val time = LogUtils.getLogTime(message)
        val map = MutableMap[String, String]("Time" -> time)
        for (col <- LOG_COLS) {
          val value = LogUtils.getLogValue(message, col, "")
          if (col == "Operate") {
            val step = OPERATE_MAP.getOrElse(col, "1")
            map("Step") = step
          } else {
            map(col) = value
          }
        }
        val sdkID = LogUtils.getLogValue(message, "SDKId", "")
        val platformID = LogUtils.getPlatformIDBySDKID(sdkID, sdkMap)
        platformResults(platformID) = platformResults.getOrElse(platformID, ArrayBuffer[MutableMap[String, String]]())
        if (StringUtils.isNotBlank(map("IMEI"))) {
          platformResults(platformID) += map
        }
      }
    }
    //记录入库
    for ((platformID, platformResult) <- platformResults) {
      DBManager.batchInsertByDate(platformID, platformResult, DB_COLS, TBL_NAME)
    }
  }
}