package com.yuhe.mgame.statics

import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson.JSONObject

trait StaticsTrait {
  /**
   * 实时统计逻辑
   */
  def execute(logList: ArrayBuffer[JSONObject], serverMap: collection.mutable.Map[String, ArrayBuffer[String]], sdkMap: collection.mutable.Map[String, String])
}