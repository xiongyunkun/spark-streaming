package com.yuhe.mgame.log

import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONObject
import collection.mutable.{ Map => MutableMap }
import com.yuhe.mgame.utils.DateUtils2
import com.yuhe.mgame.db.OnlineDB

object Online extends LogTrait {

  def parseLog(logList: ArrayBuffer[JSONObject], serverMap: collection.mutable.Map[String, ArrayBuffer[String]], sdkMap: collection.mutable.Map[String, String]) {
    val platformResults = MutableMap[String, ArrayBuffer[Map[String, String]]]()
    for (log <- logList) {
      val onlineNum = log.getString("num")
      val hostID = log.getString("hostid")
      val time = log.getString("time")
      val floorTime = DateUtils2.getFloorTime(time.toInt)
      if (onlineNum.toInt > 0 && serverMap.contains(hostID)) {
        val platformList = serverMap(hostID)
        for (platformID <- platformList) {
          val map = Map[String, String](
            "HostID" -> hostID,
            "PlatformID" -> hostID,
            "Time" -> floorTime,
            "OnlineNum" -> onlineNum)
          platformResults(platformID) = platformResults.getOrElse(platformID, ArrayBuffer[Map[String, String]]())
          platformResults(platformID) += map
        }
      }
    }
    //记录入库
    for ((platformID, platformResult) <- platformResults) {
      OnlineDB.insert(platformID, platformResult)
    }
  }
}