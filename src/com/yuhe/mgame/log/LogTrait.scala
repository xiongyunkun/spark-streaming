package com.yuhe.mgame.log

import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONObject

trait LogTrait {
  def parseLog(logList: ArrayBuffer[JSONObject], serverMap: collection.mutable.Map[String, String], sdkMap: collection.mutable.Map[String, String])
}